extern crate mux;
extern crate time;

use mux::session::*;

extern crate byteorder;
extern crate rand;

use mux::Rmsg;

use std::cmp::max;
use std::net::TcpStream;
use std::sync::Arc;

use std::thread;
use std::time::Duration;

fn test_session(socket: TcpStream) {
    println!("Testing mux client session.");

    let session = Arc::new(MuxSession::new(socket).unwrap());

    let iters = 10_000;
    let threadc = 50;

    let startt = time::get_time();

    let threads: Vec<thread::JoinHandle<Duration>> = (0..threadc).map(|id| {
        let session = session.clone();

        thread::spawn(move || {
            let mut ping_time = Duration::new(0, 0);
            let mut pingc = 0;
            for _ in 0..iters {
                if rand::random::<u8>() > 64 {
                    let b = format!("Hello, world: {}", id).into_bytes();
                    let frame = mux::Tdispatch::basic_("/foo".to_string(), b);

                    let msg = session.dispatch(&frame).unwrap();
                    if let Rmsg::Ok(body) = msg.msg {
                        let _ = String::from_utf8(body).unwrap();
                    } else {
                        panic!("Error during mux request!");
                    }
                } else {
                    ping_time = ping_time + session.ping().unwrap();
                    pingc += 1;
                }
            }

            ping_time/max(1, pingc)
        })
    }).collect();

    let mut total_ping = Duration::new(0, 0);
    let threadc = threads.len() as u32;
    for t in threads {
        total_ping = total_ping + t.join().unwrap();
    }

    let rps = {
        let elapsed = time::get_time() - startt;
        ((iters*threadc) as f32/(elapsed.num_milliseconds() as f32)) * 1e3
    };

    println!("Finished. Rps: {}. Mean Ping: {:?}", rps, total_ping/threadc);
}

fn main() {
  let socket = TcpStream::connect(("localhost", 9000)).unwrap();
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

        let status = Command::new("cargo")
            .args(&["test", "-p", subcrate])
            .status()
            .unwrap();

        assert!(status.success(),
                "test for sub-crate: {} returned: {:?}",
                subcrate,
                status.code().unwrap());
    }
}
