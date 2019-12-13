#[macro_use]
extern crate log;

extern crate env_logger;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::from_utf8;

fn main() {
    env_logger::init();
    let server_add = "localhost:8080";

    match TcpStream::connect(server_add) {
        Ok(mut stream) => {
            info!("Successfully connected to server: {}", server_add);

            let msg = "PING\r\n";

            stream.write(msg.as_bytes()).unwrap();
            info!("Sent {}, awaiting reply...", msg);

            let mut data = [0 as u8; 1024];
            match stream.read(&mut data) {
                Ok(_) => {
                    let text = from_utf8(&data).unwrap();
                    info!("reply: {}", text);
                }
                Err(e) => {
                    error!("Failed to receive data: {}", e);
                }
            }
        }
        Err(e) => {
            error!("Failed to connect: {}", e);
        }
    }
    info!("Terminated.");
}
