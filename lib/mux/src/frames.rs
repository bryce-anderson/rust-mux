

extern crate sharedbuffer;
extern crate byteorder;

use byteorder::{ReadBytesExt, BigEndian, WriteBytesExt};

use std::io;
use std::io::{ErrorKind, Read, Write};

use sharedbuffer::SharedReadBuffer;
use super::*;

use std::{i16, u16};

pub fn encode_contexts<W: Write + ?Sized>(buffer: &mut W, contexts: &Contexts) -> Result<()> {
    // TODO: these shouldn't be asserts.
    assert!(contexts.len() <= u16::MAX as usize);
    tryb!(buffer.write_u16::<BigEndian>(contexts.len() as u16));
    
    for &(ref k, ref v) in contexts {
        assert!(k.len() <= u16::MAX as usize);
        assert!(v.len() <= u16::MAX as usize);

        tryb!(buffer.write_u16::<BigEndian>(k.len() as u16));
        tryi!(buffer.write_all(&k[..]));

        tryb!(buffer.write_u16::<BigEndian>(v.len() as u16));
        tryi!(buffer.write_all(&v[..]));
    }

    Ok(())
}

pub fn decode_contexts<R: Read + ?Sized>(buffer: &mut R) -> Result<Contexts> {
    let len = tryb!(buffer.read_u16::<BigEndian>());

    let mut acc = Vec::new();

    for _ in 0..len {
        let key_len = tryb!(buffer.read_u16::<BigEndian>());
        let mut key = vec![0;key_len as usize];
        tryi!(buffer.read_exact(&mut key[..]));

        let val_len = tryb!(buffer.read_u16::<BigEndian>());
        let mut val = vec![0;val_len as usize];
        tryi!(buffer.read_exact(&mut val[..]));
        acc.push((key,val));
    }

    Ok(acc)
}

pub fn decode_dtable<R: Read + ?Sized>(buffer: &mut R) -> Result<DTable> {
    let ctxs : Vec<(Vec<u8>,Vec<u8>)> = try!(decode_contexts(buffer));
    let mut acc = Vec::with_capacity(ctxs.len());

    for (k,v) in ctxs {
        let k = try!(to_string(k));
        let v = try!(to_string(v));
        acc.push((k,v));
    }

    Ok(DTable{ entries: acc })
}

pub fn encode_dtable<R: Write + ?Sized>(buffer: &mut R, table: &DTable) -> Result<()> {
    tryb!(buffer.write_i16::<BigEndian>(table.entries.len() as i16));

    for &(ref k, ref v) in &table.entries {
        try!(encode_string(buffer, k));
        try!(encode_string(buffer, v));
    }
    Ok(())
}

fn to_string(vec: Vec<u8>) -> Result<String> {
    match String::from_utf8(vec) {
        Ok(s) => Ok(s),
        Err(_) => Err(Error::Error(
                      io::Error::new(ErrorKind::Other, "Failed to decode UTF8")
                      )),
    }
}

// decode a utf8 string with length specified by a u16 prefix byte
fn decode_string<R: Read + ?Sized>(buffer: &mut R) -> Result<String> {
    let str_len = tryb!(buffer.read_u16::<BigEndian>());
    let mut s = vec![0; str_len as usize];

    tryi!(buffer.read_exact(&mut s));

    to_string(s)
}

#[inline]
fn encode_string<W: Write + ?Sized>(buffer: &mut W, s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    assert!(bytes.len() <= i16::MAX as usize);
    tryb!(buffer.write_i16::<BigEndian>(bytes.len() as i16));
    tryi!(buffer.write_all(bytes));
    Ok(())
}

pub fn encode_init(buffer: &mut Write, msg: &Init) -> Result<()> {
    tryb!(buffer.write_i16::<BigEndian>(msg.version));

    for &(ref k, ref v) in &msg.headers {
        tryb!(buffer.write_i32::<BigEndian>(k.len() as i32));
        tryi!(buffer.write_all(k));
        tryb!(buffer.write_i32::<BigEndian>(v.len() as i32));
        tryi!(buffer.write_all(v));
    }

    Ok(())
}

pub fn decode_init(mut buffer: SharedReadBuffer) -> Result<Init> {
    let version = tryb!(buffer.read_i16::<BigEndian>());

    let mut headers = Vec::new();
    while buffer.remaining() > 0 {
        let klen = tryb!(buffer.read_i32::<BigEndian>());
        let mut k = vec![0;klen as usize];
        tryi!(buffer.read_exact(&mut k));

        let vlen = tryb!(buffer.read_i32::<BigEndian>());
        let mut v = vec![0;vlen as usize];
        tryi!(buffer.read_exact(&mut v));

        headers.push((k,v));
    }

    Ok(Init { version: version, headers: headers })
}

pub fn encode_rdispatch(buffer: &mut Write, msg: &Rdispatch) -> Result<()> {
    tryb!(buffer.write_i8(msg.status));
    try!(encode_contexts(buffer, &msg.contexts));
    tryi!(buffer.write_all(&msg.body));

    Ok(())
}

pub fn decode_rdispatch(mut buffer: SharedReadBuffer) -> Result<Rdispatch> {
    let status = tryb!(buffer.read_i8());
    let contexts = try!(decode_contexts(&mut buffer));
    let body = buffer.consume_remaining();

    Ok( Rdispatch {
        status: status,
        contexts: contexts,
        body: body
    })
}

// Expects to receive a SharedReadBuffer that consists of the entire message
pub fn decode_tdispatch(mut buffer: SharedReadBuffer) -> Result<Tdispatch> {
    let contexts = try!(decode_contexts(&mut buffer));
    let dest = try!(decode_string(&mut buffer));
    let dtable = try!(decode_dtable(&mut buffer));
    let body = buffer.consume_remaining();

    Ok(Tdispatch {
        contexts: contexts,
        dest: dest,
        dtable: dtable,
        body: body,
    })
}


pub fn encode_tdispatch(buffer: &mut Write, msg: &Tdispatch) -> Result<()> {
    try!(encode_contexts(buffer, &msg.contexts));
    try!(encode_string(buffer, &msg.dest));
    try!(encode_dtable(buffer, &msg.dtable));
    tryi!(buffer.write_all(&msg.body));

    Ok(())
}
