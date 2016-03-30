extern crate byteorder;

use std::io;
use std::io::{Cursor, Read, Write};

use std::time::Duration;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

pub type Headers = Vec<(String, Vec<u8>)>;
pub type Contexts = Vec<(Vec<u8>, Vec<u8>)>;

pub mod types {
    pub const TREQ: i8 = 1;
    pub const RREQ: i8 = -1;

    pub const TDISPATCH: i8 = 2;
    pub const RDISPATCH: i8 = -2;

    pub const TINIT: i8 = 68;
    pub const RINIT: i8 = -68;

    pub const TDRAIN: i8 = 64;
    pub const RDRAIN: i8 = -64;

    pub const TPING: i8 = 65;
    pub const RPING: i8 = -65;

    pub const TDISCARDED: i8 = 66;
    pub const TLEASE: i8 = 67;

    pub const RERR: i8 = -128;
}

// extract a value from the byteorder::Result
macro_rules! tryb {
    ($e:expr) => (
        match $e {
            Ok(r) => r,
            Err(byteorder::Error::UnexpectedEOF) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "End of input"
                ));
            }
            Err(byteorder::Error::Io(err)) => {
                return Err(err);
            }
        }
    )
}

#[derive(Debug)]
pub struct MuxPacket {
    pub tpe: i8,
    pub tag: Tag,
    pub buffer: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Tag {
    pub end: bool,
    pub id: u32,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Dtab {
    pub entries: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct Message {
    pub tag: Tag,
    pub frame: MessageFrame,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MessageFrame {
    Treq(Treq),
    Rreq(Rmsg),
    Tdispatch(Tdispatch),
    Rdispatch(Rdispatch),
    Tinit(Init),
    Rinit(Init),
    Tdrain,
    Rdrain,
    Tping,
    Rping,
    Rerr(String),
    Tlease(Duration),   // Notification of a lease of resources for the specified duration
    // Tdiscarded(String), // Sent by a client to alert the server of a discarded message
}

#[derive(PartialEq, Eq, Debug)]
pub struct Treq {
    pub headers: Headers,
    pub body: Vec<u8>,
}

impl Treq {
    pub fn frame_size(&self) -> usize {
        let mut size = 1; // header count
        for &(ref k, ref v) in &self.headers {
            size += 2; // key and value lengths
            size += k.as_bytes().len();
            size += v.len();
        }

        size + self.body.len()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct Tdispatch {
    pub contexts: Contexts,
    pub dest: String,
    pub dtab: Dtab,
    pub body: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Rdispatch {
    pub contexts: Contexts,
    pub msg: Rmsg,
}

#[derive(PartialEq, Eq, Debug)]
pub enum Rmsg {
    Ok(Vec<u8>),
    Error(String),
    Nack(String),
}

impl Rmsg {
    pub fn frame_size(&self) -> usize {
        match self {
            &Rmsg::Ok(ref b) => b.len(),
            &Rmsg::Error(ref m) => m.as_bytes().len(),
            &Rmsg::Nack(ref m) => m.as_bytes().len(),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct Init {
    pub version: u16,
    pub headers: Contexts,
}

impl Message {
    #[inline]
    pub fn end(id: u32, frame: MessageFrame) -> Message {
        Message {
            tag: Tag::new(true, id),
            frame: frame,
        }
    }
}

impl Tag {
    #[inline]
    pub fn new(end: bool, id: u32) -> Tag {
        Tag {
            end: end,
            id: id,
        }
    }

    pub fn decode_tag(r: &mut Read) -> io::Result<Tag> {
        let mut bts = [0; 3];
        let _ = try!(r.read(&mut bts));

        let id = (!(1 << 23)) &  // clear the last bit, its for the end flag
                ((bts[0] as u32) << 16 |
                 (bts[1] as u32) <<  8 |
                 (bts[2] as u32));

        Ok(Tag {
            end: (1 << 7) & bts[0] != 0,
            id: id,
        })
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8;3] {
        let endbit = if self.end { 1 } else { 0 };
        [(self.id >> 16 & 0x7f) as u8 | (endbit << 7),
            (self.id >> 8 & 0xff) as u8,
            (self.id >> 0 & 0xff) as u8]
    }

    #[inline]
    pub fn encode_tag(buffer: &mut Write, tag: &Tag) -> io::Result<()> {
        let bts = tag.to_bytes();
        try!(buffer.write_all(&bts));
        Ok(())
    }
}

impl Init {
    pub fn frame_size(&self) -> usize {
        let mut size = 2; // version

        for &(ref k, ref v) in &self.headers {
            // each value preceeded by its len (i32)
            size += 8 + k.len() + v.len();
        }
        size
    }
}

impl Dtab {
    #[inline]
    pub fn new() -> Dtab {
        Dtab::from(Vec::new())
    }

    #[inline]
    pub fn from(entries: Vec<(String, String)>) -> Dtab {
        Dtab { entries: entries }
    }

    #[inline]
    pub fn add_entry(&mut self, key: String, value: String) {
        self.entries.push((key, value));
    }
}

impl MessageFrame {
    pub fn frame_size(&self) -> usize {
        match self {
            &MessageFrame::Treq(ref f) => f.frame_size(),
            &MessageFrame::Rreq(ref f) => f.frame_size(),
            &MessageFrame::Tdispatch(ref f) => f.frame_size(),
            &MessageFrame::Rdispatch(ref f) => f.frame_size(),
            &MessageFrame::Tinit(ref f) => f.frame_size(),
            &MessageFrame::Rinit(ref f) => f.frame_size(),
            &MessageFrame::Tdrain => 0,
            &MessageFrame::Rdrain => 0,
            &MessageFrame::Tping => 0,
            &MessageFrame::Rping => 0,
            &MessageFrame::Tlease(_) => 9,
            &MessageFrame::Rerr(ref msg) => msg.as_bytes().len(),
        }
    }

    pub fn frame_id(&self) -> i8 {
        match self {
            &MessageFrame::Treq(_) => types::TREQ,
            &MessageFrame::Rreq(_) => types::RREQ,
            &MessageFrame::Tdispatch(_) => types::TDISPATCH,
            &MessageFrame::Rdispatch(_) => types::RDISPATCH,
            &MessageFrame::Tinit(_) => types::TINIT,
            &MessageFrame::Rinit(_) => types::RINIT,
            &MessageFrame::Tdrain => types::TDRAIN,
            &MessageFrame::Rdrain => types::RDRAIN,
            &MessageFrame::Tping => types::TPING,
            &MessageFrame::Rping => types::RPING,
            &MessageFrame::Tlease(_) => types::TLEASE,
            &MessageFrame::Rerr(_) => types::RERR,
        }
    }
}

#[inline]
fn context_size(contexts: &Contexts) -> usize {
    let mut size = 2; // context size

    for &(ref k, ref v) in contexts {
        size += 4; // two lengths
        size += k.len();
        size += v.len();
    }
    size
}

#[inline]
fn dtab_size(table: &Dtab) -> usize {
    let mut size = 2; // context size

    for &(ref k, ref v) in &table.entries {
        size += 4; // the two lengths
        size += k.as_bytes().len();
        size += v.as_bytes().len();
    }

    size
}

impl Tdispatch {
    fn frame_size(&self) -> usize {
        let mut size = 2 + // dest size
                       context_size(&self.contexts) +
                       dtab_size(&self.dtab);

        size += self.dest.as_bytes().len();
        size += self.body.len();
        size
    }

    pub fn basic_(dest: String, body: Vec<u8>) -> Tdispatch {
        Tdispatch {
            contexts: Vec::new(),
            dest: dest,
            dtab: Dtab::new(),
            body: body,
        }
    }

    pub fn basic(dest: String, body: Vec<u8>) -> MessageFrame {
        MessageFrame::Tdispatch(Tdispatch::basic_(dest, body))
    }
}

impl Rdispatch {
    fn frame_size(&self) -> usize {
        1 + context_size(&self.contexts) + match &self.msg {
            &Rmsg::Ok(ref body) => body.len(),
            &Rmsg::Error(ref msg) => msg.as_bytes().len(),
            &Rmsg::Nack(ref msg) => msg.as_bytes().len(),
        }
    }
}

// write the message to the Write
pub fn encode_message(buffer: &mut Write, msg: &Message) -> io::Result<()> {
    // the size is the buffer size + the header (id + tag)
    tryb!(buffer.write_i32::<BigEndian>(msg.frame.frame_size() as i32 + 4));
    tryb!(buffer.write_i8(msg.frame.frame_id()));
    try!(Tag::encode_tag(buffer, &msg.tag));

    encode_frame(buffer, &msg.frame)
}

pub fn encode_frame(buffer: &mut Write, frame: &MessageFrame) -> io::Result<()> {
    match frame {
        &MessageFrame::Treq(ref f) => frames::encode_treq(buffer, f),
        &MessageFrame::Rreq(ref f) => frames::encode_rreq(buffer, f),
        &MessageFrame::Tdispatch(ref f) => frames::encode_tdispatch(buffer, f),
        &MessageFrame::Rdispatch(ref f) => frames::encode_rdispatch(buffer, f),
        &MessageFrame::Tinit(ref f) => frames::encode_init(buffer, f),
        &MessageFrame::Rinit(ref f) => frames::encode_init(buffer, f),
        // the following are empty messages
        &MessageFrame::Tping => Ok(()),
        &MessageFrame::Rping => Ok(()),
        &MessageFrame::Tdrain => Ok(()),
        &MessageFrame::Rdrain => Ok(()),
        &MessageFrame::Tlease(ref d) => {
            let millis = d.as_secs()*1000 + (((d.subsec_nanos() as f64)/1e6) as u64);
            tryb!(buffer.write_u8(0));
            tryb!(buffer.write_u64::<BigEndian>(millis));
            Ok(())
        }
        &MessageFrame::Rerr(ref msg) => {
            buffer.write_all(msg.as_bytes())
        }
    }
}

// Decodes `data` into a frame if type `tpe`
pub fn decode_frame(tpe: i8, data: &[u8]) -> io::Result<MessageFrame> {
    Ok(match tpe {
        types::TREQ => MessageFrame::Treq(try!(frames::decode_treq(data))),
        types::RREQ => MessageFrame::Rreq(try!(frames::decode_rreq(data))),
        types::TDISPATCH => MessageFrame::Tdispatch(try!(frames::decode_tdispatch(data))),
        types::RDISPATCH => MessageFrame::Rdispatch(try!(frames::decode_rdispatch(data))),
        types::TINIT => MessageFrame::Tinit(try!(frames::decode_init(data))),
        types::RINIT => MessageFrame::Rinit(try!(frames::decode_init(data))),
        types::TDRAIN => MessageFrame::Tdrain,
        types::RDRAIN => MessageFrame::Rdrain,
        types::TPING => MessageFrame::Tping,
        types::RPING => MessageFrame::Rping,
        types::TLEASE => {
            let mut buffer = Cursor::new(data);
            let _ = try!(buffer.read_u8());
            let ticks = try!(buffer.read_u64::<BigEndian>());
            MessageFrame::Tlease(Duration::from_millis(ticks))
        }
        types::RERR => MessageFrame::Rerr(try!(frames::decode_rerr(data))),
        other => {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput,
                    format!("Invalid frame type: {}", other))
                );
        }
    })
}

// Read an entire frame buffer
pub fn read_frame(input: &mut Read) -> io::Result<MuxPacket> {
    let size = {
        let size = tryb!(input.read_i32::<BigEndian>());
        if size < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData, "Invalid mux frame size"
            ));
        }
        size as usize
    };

    let tpe = tryb!(input.read_i8());
    let tag = try!(Tag::decode_tag(input));

    let mut buf = vec![0;size-4];
    try!(input.read_exact(&mut buf));
    Ok(MuxPacket {
        tpe: tpe,
        tag: tag,
        buffer: buf,
    })
}

// This is a synchronous function that will read a whole message from the `Read`
pub fn read_message(input: &mut Read) -> io::Result<Message> {
    let packet = try!(read_frame(input));
    decode_message(&packet)
}

// expects a SharedReadBuffer of the whole mux frame
pub fn decode_message(input: &MuxPacket) -> io::Result<Message> {
    let frame = try!(decode_frame(input.tpe, &input.buffer));
    Ok(Message {
        tag: input.tag.clone(),
        frame: frame,
    })
}

pub mod frames;
pub mod session;

#[cfg(test)]
mod tests;
