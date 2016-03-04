#![crate_type = "lib"]
#![crate_name = "sharedbuffer"]

use std::sync::Arc;
use std::cmp;
use std::fmt;

use std::io;
use std::ops::Deref;
use std::io::{ErrorKind, Read, Seek, SeekFrom};


/* TODO: this would be better if we didn't indirect twice to the data
 * but that would require our own version of Arc and Vec... It shouldn't
 * be that bad but its just not important right now.
 */
#[derive(Clone,Debug)]
pub struct SharedReadBuffer {
  inner: Arc<Vec<u8>>,
  offset: usize, // lower limit of where we can look
  pos: usize,    // absolute position
  limit: usize   // absolute end
}

impl SharedReadBuffer {
    pub fn empty() -> SharedReadBuffer {
        SharedReadBuffer::new(Vec::new())
    }

  pub fn new(buffer: Vec<u8>) -> SharedReadBuffer {
    let lim = buffer.len();

    SharedReadBuffer {
      inner: Arc::new(buffer),
      offset: 0,
      pos: 0,
      limit: lim
    }
  }

  pub fn as_bytes(&self) -> &[u8] {
    &self.inner[self.pos..self.limit]
  }

  pub fn consume_slice(&mut self, length: usize) -> io::Result<SharedReadBuffer> {
    let next = self.slice(length);
    let _ = try!(self.seek(SeekFrom::Current(length as i64)));
    Ok(next)
  }

  pub fn consume_remaining(self) -> SharedReadBuffer {
    SharedReadBuffer {
        inner: self.inner,
        offset: self.pos,
        pos: self.pos,
        limit: self.limit
    }
  }

  pub fn slice(&self, length: usize) -> SharedReadBuffer {
    SharedReadBuffer {
      inner: self.inner.clone(),
      offset: self.pos,
      pos: self.pos,
      limit: cmp::min(self.limit, self.pos + length)
    }
  }

 #[inline]
  pub fn len(&self) -> usize {
    self.limit - self.offset
  }

  #[inline]
  pub fn remaining(&self) -> usize {
    self.limit - self.pos
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
      self.remaining() > 0
  }

  #[inline]
  pub fn pos(&self) -> usize {
    self.pos - self.offset
  }

  pub fn peak_reader<F,T>(&self, f: F) -> T
    where F: Fn(&mut SharedReadBuffer) -> T {
    let mut image = self.clone(); // TODO: make more efficient
    f(&mut image)
  }
}

impl fmt::Display for SharedReadBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SharedReadBuffer(length: {}, pos: {})",
               self.len(), self.pos())
    }
}

impl cmp::Eq for SharedReadBuffer {}

impl cmp::PartialEq for SharedReadBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() &&
        self.pos() == other.pos() &&
        self.as_bytes() == other.as_bytes()
    }
}

impl Deref for SharedReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}


impl io::Read for SharedReadBuffer {
  fn read(&mut self, buff: &mut [u8]) -> io::Result<usize> {
    let to_read = cmp::min(buff.len(), self.limit - self.pos);
    // copy the bytes
    unsafe {
        let ptr = self.inner.as_ptr().offset(self.pos as isize);
        std::ptr::copy(ptr, buff.as_mut_ptr(), to_read);
    }
    self.pos += to_read;

    Ok(to_read)
  }
}

impl io::Seek for SharedReadBuffer {
  fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {

    let next_pos: i64 = match pos {
      SeekFrom::Start(p) => (self.offset as i64) + (p as i64),
      SeekFrom::End(p) => (self.limit as i64) + p,
      SeekFrom::Current(p) => (self.pos as i64) + p
    };

    if next_pos < (self.offset as i64) {
      Err(io::Error::new(ErrorKind::InvalidInput,
             "Invalid seek to a negative position"))
    } else {
      let next_pos = cmp::min(next_pos as usize, self.limit);
      self.pos = next_pos;
      Ok(self.pos() as u64)
    }
  }
}

#[test]
fn read() {
  use std::io::Read;

  let v = vec![0,1,2,3];
  let mut b = SharedReadBuffer::new(v);

  let mut bts = [0;2];
  b.read(&mut bts).unwrap();

  assert_eq!(&bts, &[0,1]);

  b.read(&mut bts).unwrap();
  assert_eq!(&bts, &[2,3]);
}

#[test]
fn can_slice() {
  use std::io::Read;

  let v = vec![0,1,2,3];
  let b = SharedReadBuffer::new(v);

  assert_eq!(b.len(), 4);
  assert_eq!(b.remaining(), 4);
  assert_eq!(b.pos(), 0);

  let mut c = b.slice(2);
  assert_eq!(c.len(), 2);
  assert_eq!(c.remaining(), 2);

  // need some [u8] space
  let mut space = [0;2];

  assert_eq!(c.read(&mut space).unwrap(), 2);

  assert_eq!(c.len(), 2);
  assert_eq!(c.remaining(), 0);
}

#[test]
fn can_seek() {
  use std::io::Seek;

  let v = (0..10).collect();

  let mut b = SharedReadBuffer::new(v);
  assert_eq!(b.seek(SeekFrom::Start(2)).unwrap(), 2);
  assert_eq!(b.remaining(), 8);
  assert_eq!(b.len(), 10);

  // seek back to the start
  assert_eq!(b.seek(SeekFrom::Current(-2)).unwrap(), 0);
  // cant go before 0
  assert!(b.seek(SeekFrom::Current(-2)).is_err());
  assert!(b.seek(SeekFrom::End(-20)).is_err());
  assert_eq!(b.seek(SeekFrom::End(-10)).unwrap(), 0);

  assert_eq!(b.seek(SeekFrom::Start(100)).unwrap(), 10);
  assert_eq!(b.remaining(), 0);
}

#[test]
fn seek_then_slice() {
  use std::io::Seek;

  let mut b = SharedReadBuffer::new((0..10).collect());

  assert_eq!(b.seek(SeekFrom::Start(2)).unwrap(), 2);

  let c = b.slice(8);

  assert_eq!(c.remaining(), 8);
  assert_eq!(c.len(), 8);
  assert_eq!(c.pos(), 0);

  let mut c = b.slice(4);
  assert_eq!(c.remaining(), 4);
  assert_eq!(c.len(), 4);
  assert_eq!(c.pos(), 0);

  assert_eq!(c.seek(SeekFrom::Start(2)).unwrap(), 2);
  assert_eq!(c.remaining(), 2);
  assert_eq!(c.len(), 4);
  assert_eq!(c.pos(), 2);
}

#[test]
fn slice_then_seek() {
  use std::io::Seek;

  let mut b = SharedReadBuffer::new((0..10).collect());
  assert_eq!(b.seek(SeekFrom::Start(2)).unwrap(), 2);

  let mut c = b.slice(4);
  assert_eq!(c.pos(), 0);
  assert_eq!(c.len(), 4);
  assert_eq!(c.remaining(), 4);

  assert_eq!(c.seek(SeekFrom::Current(2)).unwrap(), 2);
  assert_eq!(c.pos(), 2);
  assert_eq!(c.len(), 4);
  assert_eq!(c.remaining(), 2);

}
