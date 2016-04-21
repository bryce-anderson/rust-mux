mod sessionimpl;

use std::io;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use std::marker;

use self::sessionimpl::MuxSessionImpl;
use super::{Tdispatch, Rdispatch};

pub struct MuxSession {
    inner: Arc<MuxSessionImpl>
}

unsafe impl marker::Send for MuxSession {}
unsafe impl marker::Sync for MuxSession {}


impl MuxSession {
    pub fn new(socket: TcpStream) -> io::Result<MuxSession> {
        let inner = Arc::new(try!(MuxSessionImpl::new(socket)));
        Ok(MuxSession { inner: inner })
    }

    pub fn dispatch(&self, msg: &Tdispatch) -> io::Result<Rdispatch> {
        self.inner.dispatch(msg)
    }

    #[inline]
    pub fn ping(&self) -> io::Result<Duration> {
        self.inner.ping()
    }
}
