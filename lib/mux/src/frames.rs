extern crate byteorder;

use byteorder::{ReadBytesExt, BigEndian, WriteBytesExt};

use std::io;
use std::io::{Cursor, ErrorKind, Read, Write};

use super::*;

use std::{u8, u16};

pub fn encode_headers(buffer: &mut Write, headers: &Headers) -> io::Result<()> {
    if headers.len() > u8::MAX as usize {
        return Err(io::Error::new(
            ErrorKind::InvalidInput, format!("Too many headers: {}", headers.len())
        ));
    }

    try!(buffer.write_u8(headers.len() as u8));

    for &(ref k, ref v) in headers {
        if v.len() > u8::MAX as usize {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Invalid header size"));
        }

        try!(buffer.write_u8(*k));
        try!(buffer.write_u8(v.len() as u8));
        try!(buffer.write_all(v));
    }
    Ok(())
}

pub fn decode_headers<R: Read + ?Sized>(buffer: &mut R) -> io::Result<Headers> {
    let len = tryb!(buffer.read_u8()) as usize;
    let mut acc = Vec::with_capacity(len);

    for _ in 0..len {
        let key = tryb!(buffer.read_u8());
        let val_len = tryb!(buffer.read_u8());
        let mut val = vec![0;val_len as usize];
        try!(buffer.read_exact(&mut val[..]));
        acc.push((key, val));
    }

    Ok(acc)
}

pub fn encode_contexts(buffer: &mut Write, contexts: &Contexts) -> io::Result<()> {
    if contexts.len() > u16::MAX as usize {
        return Err(io::Error::new(ErrorKind::InvalidInput, "Too many contexts to encode"));

    }

    tryb!(buffer.write_u16::<BigEndian>(contexts.len() as u16));

    for &(ref k, ref v) in contexts {
        if k.len() > u16::MAX as usize || v.len() > u16::MAX as usize {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Context too large to encode"));
        }

        tryb!(buffer.write_u16::<BigEndian>(k.len() as u16));
        try!(buffer.write_all(&k[..]));

        tryb!(buffer.write_u16::<BigEndian>(v.len() as u16));
        try!(buffer.write_all(&v[..]));
    }

    Ok(())
}

pub fn decode_contexts<R: Read + ?Sized>(buffer: &mut R) -> io::Result<Contexts> {
    let len = tryb!(buffer.read_u16::<BigEndian>()) as usize;

    let mut acc = Vec::with_capacity(len);

    for _ in 0..len {
        let key_len = tryb!(buffer.read_u16::<BigEndian>());
        let mut key = vec![0;key_len as usize];
        try!(buffer.read_exact(&mut key[..]));

        let val_len = tryb!(buffer.read_u16::<BigEndian>());
        let mut val = vec![0;val_len as usize];
        try!(buffer.read_exact(&mut val[..]));
        acc.push((key, val));
    }

    Ok(acc)
}

pub fn decode_dtab<R: Read + ?Sized>(buffer: &mut R) -> io::Result<Dtab> {
    let ctxs: Vec<(Vec<u8>, Vec<u8>)> = try!(decode_contexts(buffer));
    let mut acc = Vec::with_capacity(ctxs.len());

    for (k, v) in ctxs {
        let k = try!(to_string(k));
        let v = try!(to_string(v));
        acc.push((k, v));
    }

    Ok(Dtab { entries: acc })
}

pub fn encode_dtab<R: Write + ?Sized>(buffer: &mut R, table: &Dtab) -> io::Result<()> {
    tryb!(buffer.write_u16::<BigEndian>(table.entries.len() as u16));

    for &(ref k, ref v) in &table.entries {
        try!(encode_string(buffer, k));
        try!(encode_string(buffer, v));
    }
    Ok(())
}

fn to_string(vec: Vec<u8>) -> io::Result<String> {
    match String::from_utf8(vec) {
        Ok(s) => Ok(s),
        Err(_) => Err(io::Error::new(ErrorKind::InvalidData, "Invalid UTF8 field")),
    }
}

// decode a utf8 string with length specified by a u16 prefix byte
#[inline]
pub fn decode_string<R: Read + ?Sized>(buffer: &mut R) -> io::Result<String> {
    let str_len = tryb!(buffer.read_u16::<BigEndian>());
    let mut s = vec![0; str_len as usize];

    try!(buffer.read_exact(&mut s));

    to_string(s)
}

#[inline]
pub fn encode_string<W: Write + ?Sized>(buffer: &mut W, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    assert!(bytes.len() <= u16::MAX as usize);
    tryb!(buffer.write_u16::<BigEndian>(bytes.len() as u16));
    buffer.write_all(bytes)
}

pub fn decode_rerr<R: Read>(mut buffer: R) -> io::Result<String> {
    let mut data = Vec::new();
    let _ = try!(buffer.read_to_end(&mut data));
    to_string(data)
}

pub fn encode_init(buffer: &mut Write, msg: &Init) -> io::Result<()> {
    tryb!(buffer.write_u16::<BigEndian>(msg.version));

    for &(ref k, ref v) in &msg.headers {
        tryb!(buffer.write_u32::<BigEndian>(k.len() as u32));
        try!(buffer.write_all(k));
        tryb!(buffer.write_u32::<BigEndian>(v.len() as u32));
        try!(buffer.write_all(v));
    }

    Ok(())
}

pub fn decode_init(data: &[u8]) -> io::Result<Init> {
    let datalen = data.len() as u64;
    let mut buffer = Cursor::new(data);

    let version = tryb!(buffer.read_u16::<BigEndian>());
    let mut headers = Vec::new();

    while buffer.position() < datalen {
        let klen = tryb!(buffer.read_u32::<BigEndian>());
        let mut k = vec![0;klen as usize];
        try!(buffer.read_exact(&mut k));

        let vlen = tryb!(buffer.read_u32::<BigEndian>());
        let mut v = vec![0;vlen as usize];
        try!(buffer.read_exact(&mut v));

        headers.push((k, v));
    }

    Ok(Init {
        version: version,
        headers: headers,
    })
}

pub fn encode_rdispatch(buffer: &mut Write, frame: &Rdispatch) -> io::Result<()> {
    let (status, body) = rmsg_status_body(&frame.msg);

    tryb!(buffer.write_u8(status));
    try!(encode_contexts(buffer, &frame.contexts));
    buffer.write_all(body)
}

// Expects to consume the whole stream
pub fn decode_rdispatch<R: Read>(mut buffer: R) -> io::Result<Rdispatch> {
    let status = tryb!(buffer.read_u8());
    let contexts = try!(decode_contexts(&mut buffer));
    let mut body = Vec::new();
    let _ = try!(buffer.read_to_end(&mut body));

    Ok(Rdispatch {
        contexts: contexts,
        msg: try!(decode_rmsg_body(status, body)),
    })
}

pub fn encode_rreq(buffer: &mut Write, frame: &Rmsg) -> io::Result<()> {
    let (status, body) = rmsg_status_body(frame);
    tryb!(buffer.write_u8(status));
    buffer.write_all(body)
}

pub fn decode_rreq<R: Read>(mut buffer: R) -> io::Result<Rmsg> {
    let status = try!(buffer.read_u8());
    let mut body = Vec::new();
    try!(buffer.read_to_end(&mut body));
    decode_rmsg_body(status, body)
}



// Expects to consume the whole stream
pub fn decode_tdispatch<R: Read>(mut buffer: R) -> io::Result<Tdispatch> {
    let contexts = try!(decode_contexts(&mut buffer));
    let dest = try!(decode_string(&mut buffer));
    let dtab = try!(decode_dtab(&mut buffer));

    let mut body = Vec::new();
    let _ = try!(buffer.read_to_end(&mut body));

    Ok(Tdispatch {
        contexts: contexts,
        dest: dest,
        dtab: dtab,
        body: body,
    })
}

pub fn encode_tdispatch(buffer: &mut Write, msg: &Tdispatch) -> io::Result<()> {
    try!(encode_contexts(buffer, &msg.contexts));
    try!(encode_string(buffer, &msg.dest));
    try!(encode_dtab(buffer, &msg.dtab));
    buffer.write_all(&msg.body)
}

pub fn decode_treq<R: Read>(mut buffer: R) -> io::Result<Treq> {
    let headers = try!(decode_headers(&mut buffer));
    let mut body = Vec::new();

    let _ = try!(buffer.read_to_end(&mut body));
    Ok(Treq {
        headers: headers,
        body: body,
    })
}

#[inline]
pub fn encode_treq(buffer: &mut Write, msg: &Treq) -> io::Result<()> {
    try!(encode_headers(buffer, &msg.headers));
    buffer.write_all(&msg.body)
}

#[inline]
fn rmsg_status_body(msg: &Rmsg) -> (u8, &[u8]) {
    match msg {
        &Rmsg::Ok(ref body) => (0, body.as_ref()),
        &Rmsg::Error(ref msg) => (1, msg.as_bytes()),
        &Rmsg::Nack(ref msg) => (2, msg.as_bytes()),
    }
}

#[inline]
fn decode_rmsg_body(status: u8, body: Vec<u8>) -> io::Result<Rmsg> {
    match status {
        0 => Ok(Rmsg::Ok(body)),
        1 => Ok(Rmsg::Error(try!(to_string(body)))),
        2 => Ok(Rmsg::Nack(try!(to_string(body)))),
        other => Err(
            io::Error::new(ErrorKind::InvalidData, format!("Invalid status code: {}", other))
        )
    }
}
