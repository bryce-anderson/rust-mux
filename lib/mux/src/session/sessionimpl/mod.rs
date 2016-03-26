extern crate sharedbuffer;
extern crate time;

use super::super::*;

use byteorder;
use byteorder::{WriteBytesExt, BigEndian, ByteOrder};

use std::collections::BTreeMap;
use std::error::{Error};
use std::mem;
use std::net::TcpStream;

use std::io;
use std::io::{ErrorKind, Read, Write, BufReader, BufWriter};

use std::sync::{Mutex, MutexGuard, Condvar};
use std::time::Duration;
use std::thread;

// pub const DEFAULT_CHUNK_SIZE: usize = 64*1024; // 64 KB not used yet.
const MAX_TAG: usize = (1 << 23) - 1;

// need to detail how the state can be 'poisoned' by a protocol error
struct SessionReadState {
    channel_states: BTreeMap<u32, ReadState>,
    read: Option<Box<Read>>,
}

enum ReadState {
    Packet(Option<MuxPacket>),
    Waiting(*const Condvar),
    Poisoned(io::Error),
}

pub struct MuxSessionImpl {
    // order of mutexes is that as listed below. Race conditions otherwise...
    read_state: Mutex<SessionReadState>,
    write: Mutex<Box<Write>>,
}

enum Either<L,R> {
    Left(L),
    Right(R),
}

impl MuxSessionImpl {
    pub fn new(socket: TcpStream) -> io::Result<MuxSessionImpl> {
        let read = Box::new(BufReader::new(try!(socket.try_clone())));

        let read_state = Mutex::new(SessionReadState {
            channel_states: BTreeMap::new(),
            read: Some(read),
        });

        Ok(MuxSessionImpl {
            read_state: read_state,
            write: Mutex::new(Box::new(BufWriter::new(socket))),
        })
    }


    pub fn dispatch(&self, msg: &Tdispatch) -> io::Result<Rdispatch> {
        let id = try!(self.next_id());

        try!(self.wrap_write(true, id, |id, write| {
            self.dispatch_write(id, write, msg)
        }));

        let packet = try!(self.dispatch_read(id));
        frames::decode_rdispatch(packet.buffer)
    }

    pub fn ping(&self) -> io::Result<Duration> {
        let id = try!(self.next_id());
        let start = time::now();

        try!(self.wrap_write(true, id, |id, write| {
            let ping = Message {
                tag: Tag { end: true, id: id },
                frame: MessageFrame::TPing,
            };

            encode_message(&mut *write, &ping)
        }));

        let packet = try!(self.dispatch_read(id));
        let msg = try!(decode_message(packet));
        match msg.frame {
            MessageFrame::RPing => {
                let elapsed = time::now() - start;
                Ok(Duration::from_millis(elapsed.num_milliseconds() as u64))
            }
            invalid => {
                let msg = format!("Received invalid reply for ping: {:?}", invalid);
                Err(io::Error::new(ErrorKind::InvalidData, msg))
            }
        }
    }

    // wrap writing functions in logic to remove the channel from
    // the state on failure.
    fn wrap_write<F>(&self, flush: bool, id: u32,f: F) -> io::Result<()>
    where F: Fn(u32, &mut Write) -> io::Result<()> {
        let mut write = self.write.lock().unwrap();
        let result = {
            let r1 = f(id, &mut *write);
            if flush && r1.is_ok() {
                write.flush()
            } else {
                r1
            }
        };

        if result.is_err() {
            let mut read_state = self.read_state.lock().unwrap();
            let _ = read_state.channel_states.remove(&id);
        }

        result
    }

    fn dispatch_write(&self, id: u32, write: &mut Write, msg: &Tdispatch) -> io::Result<()> {
        let tag = Tag { end: true, id: id };

        tryb!(write.write_i32::<BigEndian>(msg.frame_size() as i32 + 4));
        tryb!(write.write_i8(types::TDISPATCH));

        try!(Tag::encode_tag(&mut *write, &tag));
        frames::encode_tdispatch(&mut *write, msg)
    }

    fn dispatch_read(&self, id: u32) -> io::Result<MuxPacket> {
        match self.dispatch_read_slave(id) {
            Either::Right(result) => result,
            Either::Left(read) => self.dispatch_read_master(id, read),
        }
    }

    // Wait for a result or for the Read to become available
    fn dispatch_read_slave(&self, id: u32) -> Either<Box<Read>, io::Result<MuxPacket>> {
        let mut read_state = self.read_state.lock().unwrap();
        let cv = Condvar::new(); // would be sweet if we could use a static...

        loop {
            let read_available = read_state.read.is_some();

            let result = match read_state.channel_states.get_mut(&id).unwrap() {
                &mut ReadState::Packet(ref mut packet) if packet.is_some() => {
                    // we have data
                    let mut data = None;
                    mem::swap(packet, &mut data);
                    Some(Ok(data.unwrap()))
                }
                &mut ReadState::Poisoned(ref err) => {
                    Some(Err(copy_error(err)))
                }
                    // either this is the first go, we were elected leader,
                    // or a spurious wakeup occured
                st => {
                    *st = if read_available { ReadState::Packet(None) }
                          else { ReadState::Waiting(&cv) };
                    None
                }
            };

            match result {
                Some(result) => {
                    let _ = read_state.channel_states.remove(&id);
                    return Either::Right(result);
                }
                None if read_available => {
                    let mut old =  None;
                    mem::swap(&mut read_state.read, &mut old);
                    return Either::Left(old.unwrap());
                }
                None => {
                    // wait for someone to wake us up
                    read_state = cv.wait(read_state).unwrap();
                }
            }
        }
    }

    // Become the read master, reading data and notifying waiting channel_states.
    // It is our job to clean up on error and elect a new leader once we have found
    // the packet we are interested in. We must also take care to return the Read
    // or else we will kill the session.
    fn dispatch_read_master(&self, id: u32, mut read: Box<Read>) -> io::Result<MuxPacket> {
        loop {
            // read some data
            let packet = read_frame(&mut *read);
            let mut read_state = self.read_state.lock().unwrap();

            let result = match packet {
                Err(err) => {
                    // Alert everyone...
                    for (k,v) in read_state.channel_states.iter_mut() {
                        let mut next = ReadState::Poisoned(copy_error(&err));
                        mem::swap(&mut next, v);
                        if let ReadState::Waiting(cv) = next {
                            unsafe { (*cv).notify_one(); }
                        }
                    }

                    Err(err)
                }
                Ok(packet) => {
                     if packet.tag.id == id {
                        // our packet. Need to elect a new leader
                        for (k,v) in read_state.channel_states.iter() {
                            if let &ReadState::Waiting(cv) = v {
                                    unsafe { (*cv).notify_one(); }
                                    break;
                            }
                        }

                        Ok(packet)
                    } else {
                        if let Some(st) = read_state.channel_states.get_mut(&packet.tag.id) {
                            let mut old = ReadState::Packet(Some(packet));
                            mem::swap(&mut old, st);

                            if let ReadState::Waiting(cv) = old {
                                unsafe { (*cv).notify_one(); }
                            }
                        } else {
                            println!("Warning: packet for unopen channel: {}. Dropping.", packet.tag.id);
                        }
                        continue;
                    }
                }
            };
            // cleanup code. If we get past the match, we have a result
            let _ = read_state.channel_states.remove(&id);
            read_state.read = Some(read);
            return result;
        }
    }

    fn ping_reply(&self, tag: Tag) -> io::Result<()> {
        let ping = Message {
            tag: tag,
            frame: MessageFrame::RPing,
        };

        let mut write = self.write.lock().unwrap();
        try!(encode_message(&mut *write, &ping));
        write.flush()
    }

    // deals with packets other than Rdispatch
    fn handle_packet(&self, packet: MuxPacket) -> io::Result<()> {
        match try!(decode_frame(packet.tpe, packet.buffer)) {
            MessageFrame::RPing => {
                let message = "Unexpected ping reply";
                self.abort_session(message);
                Err(io::Error::new(io::ErrorKind::InvalidData, message))
            }
            MessageFrame::TPing => self.ping_reply(packet.tag),

            MessageFrame::RDrain => panic!("Not implemented!"),
            MessageFrame::TDrain => panic!("Not implemented!"),

            MessageFrame::RInit(_) => panic!("Not implemented!"),
            MessageFrame::TInit(_) => panic!("Not implemented!"),

            MessageFrame::Tdispatch(_) => panic!("Not implemented!"),
            MessageFrame::Rdispatch(_) => {
                panic!("Not implemented!")
            }
            MessageFrame::RErr(_) => panic!("Not implemented!"),
        }
    }

    fn abort_session(&self, msg: &str) {
        panic!("abort_session not implemented")
    }

    fn next_id(&self) -> io::Result<u32> {
        let mut channel_states = &mut self.read_state.lock().unwrap().channel_states;

        for i in 2..MAX_TAG {
            let i = i as u32;
            if !channel_states.contains_key(&i) {
                channel_states.insert(i, ReadState::Packet(None));
                return Ok(i);
            }
        }
        panic!("Shouldn't get here")
    }
}

fn copy_error(err: &io::Error) -> io::Error {
    io::Error::new(err.kind(), err.description())
}
