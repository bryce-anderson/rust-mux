extern crate time;

extern crate sharedbuffer;
use sharedbuffer::SharedReadBuffer;

use super::super::*;

use byteorder;
use byteorder::{WriteBytesExt, BigEndian, ByteOrder};

use std::collections::BTreeMap;
use std::error::Error;
use std::mem;
use std::net::TcpStream;

use std::io;
use std::io::{ErrorKind, Read, Write, BufReader, BufWriter};

use std::sync::{Mutex, Condvar};
use std::time::Duration;

const MAX_TAG: usize = (1 << 23) - 1;

// Really only used in one place...
enum Either<L,R> {
    Left(L),
    Right(R),
}

// need to detail how the state can be 'poisoned' by a protocol error
struct SessionReadState {
    channel_states: BTreeMap<u32, ReadState>,
    read: Option<Box<Read>>,
    state: SessionState,
}

enum ReadState {
    Packet(Option<MuxPacket>), // a queue for later version of the protocol
    Waiting(*const Condvar),
    Poisoned(io::Error),
}

pub struct MuxSessionImpl {
    // order of mutexes is that as listed below. Race conditions otherwise...
    read_state: Mutex<SessionReadState>,
    write: Mutex<Box<Write>>,
}

enum SessionState {
    Dispatching,
    Draining,
    Closed,
    Error(ErrorKind, String),
}

impl SessionState {
    fn is_draining(&self) -> bool {
        match self {
            &SessionState::Draining => true,
            _ => false,
        }
    }
}

impl SessionReadState {
    fn new(read: Box<Read>) -> SessionReadState {
        SessionReadState {
            channel_states: BTreeMap::new(),
            read: Some(read),
            state: SessionState::Dispatching,
        }
    }

    fn next_id(&mut self) -> io::Result<u32> {
        try!(self.check_ok());

        for i in 2..MAX_TAG {
            let i = i as u32;
            if !self.channel_states.contains_key(&i) {
                self.channel_states.insert(i, ReadState::Packet(None));
                return Ok(i);
            }
        }
        panic!("Shouldn't get here")
    }

    fn release_id(&mut self, id: u32) {
        let _ = self.channel_states.remove(&id);

        if self.state.is_draining() && self.channel_states.is_empty() {
            self.state = SessionState::Closed;
        }
    }

    fn elect_leader(&self) {
        // The leader (the caller) will be in a state of Packet(None)
        for (_,v) in self.channel_states.iter() {
            if let &ReadState::Waiting(cv) = v {
                    unsafe { (*cv).notify_one(); }
                    break;
            }
        }
    }

    fn check_ok(&self) -> io::Result<()> {
        match &self.state {
            &SessionState::Dispatching => Ok(()),
            &SessionState::Error(ref k, ref r) => Err(io::Error::new(k.clone(), r.as_str())),
            &SessionState::Draining  => Err(io::Error::new(ErrorKind::ConnectionRefused, "Draining")),
            &SessionState::Closed => Err(io::Error::new(ErrorKind::BrokenPipe, "Connection closed")),
        }
    }

    fn drain(&mut self) {
        self.state = SessionState::Draining;
    }

    fn abort_session(&mut self, reason: ErrorKind, msg: &str) {
        self.state = SessionState::Error(reason, msg.to_owned());
        // We have a malformed packet or connection error. Alert the channels.
        for (_,v) in self.channel_states.iter_mut() {
            let mut next = ReadState::Poisoned(io::Error::new(reason.clone(), msg));
            mem::swap(&mut next, v);
            if let ReadState::Waiting(cv) = next {
                unsafe { (*cv).notify_one(); }
            }
        }
    }
}

impl MuxSessionImpl {
    pub fn new(socket: TcpStream) -> io::Result<MuxSessionImpl> {
        let read = Box::new(BufReader::new(try!(socket.try_clone())));
        let read_state = Mutex::new(SessionReadState::new(read));

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
        // only addresses packets intended for this channel
        match decode_frame(packet.tpe, SharedReadBuffer::new(packet.buffer)) {
            Ok(MessageFrame::Rdispatch(d)) => Ok(d),
            Ok(MessageFrame::Rerr(reason)) => Err(io::Error::new(ErrorKind::Other, reason)),

            // the rest of these are unexpected messages at this point
            Ok(other) => {
                // Tdispatch, Pings, Drains, and Inits
                let msg = format!("Unexpected frame: {:?}", &other);
                self.abort_session_proto(&msg)
            }

            Err(err) => self.abort_session(err.kind(), err.description()),
        }
    }

    pub fn ping(&self) -> io::Result<Duration> {
        let id = try!(self.next_id());
        let start = time::precise_time_ns();

        try!(self.wrap_write(true, id, |id, write| {
            let ping = Message {
                tag: Tag { end: true, id: id },
                frame: MessageFrame::Tping,
            };

            encode_message(&mut *write, &ping)
        }));

        let packet = try!(self.dispatch_read(id));
        let msg = try!(decode_message(packet));
        match msg.frame {
            MessageFrame::Rping => {
                let elapsed = time::precise_time_ns() - start;
                Ok(Duration::from_millis(elapsed / 1_000_000))
            }
            invalid => {
                let msg = format!("Received invalid reply for Ping: {:?}", invalid);
                self.abort_session_proto(&msg)
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
            read_state.release_id(id);
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

    // dispatch_read will clean up the channel state after receiving its message.
    // I don't like that pattern: the channel state should be handled at one point.
    fn dispatch_read(&self, id: u32) -> io::Result<MuxPacket> {
        match self.dispatch_read_slave(id) {
            Either::Right(result) => result,
            Either::Left(read) => self.dispatch_read_master(id, read),
        }
    }

    // Wait for a result or for the Read to become available
    fn dispatch_read_slave(&self, id: u32) -> Either<Box<Read>, io::Result<MuxPacket>> {
        let cv = Condvar::new(); // would be sweet if we could use a static...
        let mut read_state = self.read_state.lock().unwrap();

        loop {
            let read_available = read_state.read.is_some();

            // TODO: this is functionality that should be in the SessionReadState
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
                    // or a spurious wakeup occured. If we become leader set
                    // our state to an empty packet.
                st => {
                    *st = if read_available { ReadState::Packet(None) }
                          else { ReadState::Waiting(&cv) };
                    None
                }
            };

            match result {
                Some(result) => {
                    read_state.release_id(id);
                    return Either::Right(result);
                }
                None if read_available => {
                    // Becoming the leader
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
                    read_state.abort_session(err.kind(), err.description());
                    Err(err)
                }
                Ok(packet) => {
                     if packet.tag.id == id {
                        // our packet. Need to elect a new leader and return
                        read_state.elect_leader();
                        Ok(packet)
                    } else {
                        // if a channel exists, let them handle it
                        if let Some(st) = read_state.channel_states.get_mut(&packet.tag.id) {
                            let mut old = ReadState::Packet(Some(packet));
                            mem::swap(&mut old, st);

                            if let ReadState::Waiting(cv) = old {
                                unsafe { (*cv).notify_one(); }
                            }
                            continue;
                        }
                        // Note: this would better as an `else` but the borrow checker
                        //       gets upset about borrowing read_state again.
                        // If we get here, the packet is unhandled by any channel.
                        let id = packet.tag.id;
                        match decode_frame(packet.tpe, SharedReadBuffer::new(packet.buffer)) {
                            Ok(MessageFrame::Tlease(_)) if id == 0 => {
                                println!("Unhandled Tlease frame.");
                                continue;
                            }

                            Ok(MessageFrame::Tping) => {
                                match self.ping_reply(id) {
                                    Ok(_) => continue,
                                    Err(err) => {
                                        read_state.abort_session(err.kind(), err.description());
                                        Err(err)
                                    }
                                }
                            }

                            Ok(MessageFrame::Tdrain) => {
                                read_state.drain();
                                continue;
                            }

                            // All other packets are unexpected
                            Ok(frame) => {
                                let msg = format!("Unexpected frame: {:?}", &frame);
                                read_state.abort_session(ErrorKind::InvalidData, &msg);
                                Err(io::Error::new(ErrorKind::InvalidData, msg))
                            }

                            // Packet decode failure
                            Err(err) => {
                                read_state.abort_session(err.kind(), err.description());
                                Err(err)
                            }
                        }
                    }
                }
            };
            // cleanup code. If we get past the match, we have a result
            read_state.release_id(id);
            read_state.read = Some(read);
            return result;
        }
    }

    fn ping_reply(&self, id: u32) -> io::Result<()> {
        let ping = Message {
            tag: Tag { end: true, id: id },
            frame: MessageFrame::Rping,
        };

        let mut write = self.write.lock().unwrap();
        try!(encode_message(&mut *write, &ping));
        write.flush()
    }

    #[inline]
    fn abort_session_proto<T>(&self, msg: &str) -> io::Result<T> {
        self.abort_session(ErrorKind::InvalidData, msg)
    }

    // Abort the session, closing down and returning a failed result
    fn abort_session<T>(&self, reason: ErrorKind, msg: &str) -> io::Result<T> {
        let mut read_state = self.read_state.lock().unwrap();
        read_state.abort_session(reason.clone(), msg);

        Err(io::Error::new(reason, msg))
    }

    #[inline]
    fn next_id(&self) -> io::Result<u32> {
        let mut read_state = self.read_state.lock().unwrap();
        read_state.next_id()
    }
}

fn copy_error(err: &io::Error) -> io::Error {
    io::Error::new(err.kind(), err.description())
}

#[cfg(test)]
mod tst; // test helpers

// tests
#[test]
fn test_dispatch_success() {

}
