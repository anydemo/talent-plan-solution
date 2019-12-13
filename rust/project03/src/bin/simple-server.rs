#[macro_use]
extern crate log;

extern crate env_logger;

use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::str::from_utf8;
use std::thread;

fn handle_client(mut stream: TcpStream) {
    // read 20 bytes at a time from stream echoing back to stream
    loop {
        let mut read = [0; 1024];
        let mut reader = BufReader::new(&stream);
        match reader.read(&mut read) {
            Ok(n) => {
                if n == 0 {
                    // connection was closed
                    break;
                }
                let req = from_utf8(&read).expect("?");
                info!("body:\n{}|", req);

                let resp = if req.starts_with("PING") {
                    "PONG"
                } else {
                    "UNSUPPORTED CMD"
                };
                stream.write(resp.as_bytes()).unwrap();
            }
            Err(err) => {
                panic!(err);
            }
        }
    }
}

fn main() {
    env_logger::init();

    let address = "127.0.0.1:8080";
    let listener = TcpListener::bind(address).unwrap();
    info!("start server @ {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!("new conn: {}", stream.peer_addr().unwrap());
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(err) => {
                error!("error: {}", err);
            }
        }
    }
}
