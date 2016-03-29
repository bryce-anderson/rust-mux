extern crate mux;

extern crate byteorder;
extern crate rand;

use mux::session::*;

use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn test_session(socket: TcpStream) {

    let session = Arc::new(MuxSession::new(socket).unwrap());

    let res = session.ping().unwrap();
    println!("Ping time: {:?}", res);

    let threads: Vec<thread::JoinHandle<Duration>> = (0..50).map(|id| {
        let session = session.clone();

        thread::spawn(move || {
            let mut ping_time = Duration::new(0, 0);
            let iters = 10_000;
            for _ in 0..iters {
                if rand::random::<u8>() > 64 {
                    let b = format!("Hello, world: {}", id).into_bytes();
                    let frame = mux::Tdispatch::basic_("/foo".to_string(), b);

                    let resp = session.dispatch(&frame).unwrap();
                    let _ = std::str::from_utf8(&resp.body).unwrap();
                } else {
                    ping_time = ping_time + session.ping().unwrap();
                }
            }
            ping_time/(iters as u32)
        })
    }).collect();

    let mut total_ping = Duration::new(0, 0);
    let threadc = threads.len() as u32;
    for t in threads {
        total_ping = total_ping + t.join().unwrap();
    }

    println!("Finished. Average ping: {:?}", total_ping/threadc);
}

fn main() {
  let socket = TcpStream::connect(("localhost", 9000)).unwrap();

  println!("Testing TRequest frame.");
  //test_trequest(&mut socket);
  test_session(socket);
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    #[test]
    fn run() {
        test_crate("mux");
    }

    fn test_crate(subcrate: &str) {

        let status = Command::new("cargo").args(&["test", "-p", subcrate]).status().unwrap();
        assert!(status.success(),
                "test for sub-crate: {} returned: {:?}",
                subcrate,
                status.code().unwrap());
    }
}
