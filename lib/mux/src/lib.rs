extern crate byteorder;
extern crate sharedbuffer;

use sharedbuffer::SharedReadBuffer;

use std::io;
use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

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
                ))
            }
            Err(byteorder::Error::Io(err)) => {
                return Err(err)
            }
        }
    )
}

#[derive(Debug)]
pub struct MuxPacket {
    pub tpe: i8,
    pub tag: Tag,
    pub buffer: SharedReadBuffer,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Tag {
    pub end: bool,
    pub id: u32,
}

#[derive(PartialEq, Eq, Debug)]
pub struct DTable {
    pub entries: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct Message {
    pub tag: Tag,
    pub frame: MessageFrame,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MessageFrame {
    Tdispatch(Tdispatch),
    Rdispatch(Rdispatch),
    TInit(Init),
    RInit(Init),
    TDrain,
    RDrain,
    TPing,
    RPing,
    RErr(String),
}

#[derive(PartialEq, Eq, Debug)]
pub struct Tdispatch {
    pub contexts: Contexts,
    pub dest: String,
    pub dtable: DTable,
    pub body: SharedReadBuffer,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Rdispatch {
    pub status: i8,
    pub contexts: Contexts,
    pub body: SharedReadBuffer,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Init {
    pub version: i16,
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

    pub fn decode_tag<T: Read>(r: &mut T) -> io::Result<Tag> {
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

impl DTable {
    #[inline]
    pub fn new() -> DTable {
        DTable::from(Vec::new())
    }

    #[inline]
    pub fn from(entries: Vec<(String, String)>) -> DTable {
        DTable { entries: entries }
    }

    #[inline]
    pub fn add_entry(&mut self, key: String, value: String) {
        self.entries.push((key, value));
    }
}

impl MessageFrame {
    pub fn frame_size(&self) -> usize {
        match self {
            &MessageFrame::Tdispatch(ref f) => f.frame_size(),
            &MessageFrame::Rdispatch(ref f) => f.frame_size(),
            &MessageFrame::TInit(ref f) => f.frame_size(),
            &MessageFrame::RInit(ref f) => f.frame_size(),
            &MessageFrame::TDrain => 0,
            &MessageFrame::RDrain => 0,
            &MessageFrame::TPing => 0,
            &MessageFrame::RPing => 0,
            &MessageFrame::RErr(ref msg) => msg.as_bytes().len(),
        }
    }

    pub fn frame_id(&self) -> i8 {
        match self {
            &MessageFrame::Tdispatch(_) => types::TDISPATCH,
            &MessageFrame::Rdispatch(_) => types::RDISPATCH,
            &MessageFrame::TInit(_) => types::TINIT,
            &MessageFrame::RInit(_) => types::RINIT,
            &MessageFrame::TDrain => types::TDRAIN,
            &MessageFrame::RDrain => types::RDRAIN,
            &MessageFrame::TPing => types::TPING,
            &MessageFrame::RPing => types::RPING,
            &MessageFrame::RErr(_) => types::RERR,
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
fn dtable_size(table: &DTable) -> usize {
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
                       dtable_size(&self.dtable);

        size += self.dest.as_bytes().len();
        size += self.body.remaining();
        size
    }

    pub fn basic_(dest: String, body: SharedReadBuffer) -> Tdispatch {
        Tdispatch {
            contexts: Vec::new(),
            dest: dest,
            dtable: DTable::new(),
            body: body,
        }
    }

    pub fn basic(dest: String, body: SharedReadBuffer) -> MessageFrame {
        MessageFrame::Tdispatch(Tdispatch::basic_(dest, body))
    }
}

impl Rdispatch {
    fn frame_size(&self) -> usize {
        1 + context_size(&self.contexts) + self.body.remaining()
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

fn encode_frame(buffer: &mut Write, frame: &MessageFrame) -> io::Result<()> {
    match frame {
        &MessageFrame::Tdispatch(ref f) => frames::encode_tdispatch(buffer, f),
        &MessageFrame::Rdispatch(ref f) => frames::encode_rdispatch(buffer, f),
        &MessageFrame::TInit(ref f) => frames::encode_init(buffer, f),
        &MessageFrame::RInit(ref f) => frames::encode_init(buffer, f),
        // the following are empty messages
        &MessageFrame::TPing => Ok(()),
        &MessageFrame::RPing => Ok(()),
        &MessageFrame::TDrain => Ok(()),
        &MessageFrame::RDrain => Ok(()),
        &MessageFrame::RErr(ref msg) => {
            try!(buffer.write_all(msg.as_bytes()));
            Ok(())
        }
    }
}

pub fn decode_frame(id: i8, buffer: SharedReadBuffer) -> io::Result<MessageFrame> {
    Ok(match id {
        2 => MessageFrame::Tdispatch(try!(frames::decode_tdispatch(buffer))),
        -2 => MessageFrame::Rdispatch(try!(frames::decode_rdispatch(buffer))),
        68 => MessageFrame::TInit(try!(frames::decode_init(buffer))),
        -68 => MessageFrame::RInit(try!(frames::decode_init(buffer))),
        64 => MessageFrame::TDrain,
        -64 => MessageFrame::RDrain,
        65 => MessageFrame::TPing,
        -65 => MessageFrame::RPing,
        -128 => MessageFrame::RErr(try!(frames::decode_rerr(buffer))),
        other => {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput,
                    format!("Invalid frame id: {}", other))
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

    let mut buf = vec![0;size];
    try!(input.read_exact(&mut buf));
    let mut buf = SharedReadBuffer::new(buf);
    let tpe = tryb!(buf.read_i8());
    let tag = try!(Tag::decode_tag(&mut buf));
    Ok(MuxPacket {
        tpe: tpe,
        tag: tag,
        buffer: buf,
    })
}

// This is a synchronous function that will read a whole message from the `Read`
pub fn read_message(input: &mut Read) -> io::Result<Message> {
    let packet = try!(read_frame(input));
    decode_message(packet)
}

// expects a SharedReadBuffer of the whole mux frame
pub fn decode_message(input: MuxPacket) -> io::Result<Message> {
    let frame = try!(decode_frame(input.tpe, input.buffer));
    Ok(Message {
        tag: input.tag,
        frame: frame,
    })
}

pub mod frames;
pub mod session;

#[cfg(test)]
mod tests;
