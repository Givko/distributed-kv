use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::time::Duration;

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

struct ClientConnection {
    stream: TcpStream,
    write_buf: VecDeque<u8>,
    read_buf: VecDeque<u8>,
}

fn main() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let listener_std = std::net::TcpListener::bind("127.0.0.1:8080").unwrap();
    listener_std
        .set_nonblocking(true)
        .expect("could not set to nonblocking");

    let mut listener = TcpListener::from_std(listener_std);
    let mut connections: HashMap<Token, ClientConnection> = HashMap::new();
    let mut next_token_id: usize = 0;
    let token = Token(next_token_id);
    next_token_id += 1;

    _ = poll
        .registry()
        .register(&mut listener, token, Interest::READABLE);

    loop {
        _ = poll.poll(&mut events, Some(Duration::from_millis(100)));
        for event in events.iter() {
            match event.token() {
                Token(0) => {
                    if !event.is_readable() {
                        continue;
                    }

                    loop {
                        match listener.accept() {
                            Ok((mut stream, _)) => {
                                let stream_token = Token(next_token_id);
                                next_token_id += 1;

                                poll.registry()
                                    .register(&mut stream, stream_token, Interest::READABLE)
                                    .expect("Cannot register stream with token");

                                let connection = ClientConnection {
                                    stream,
                                    write_buf: VecDeque::new(),
                                    read_buf: VecDeque::new(),
                                };
                                connections.insert(stream_token, connection);
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                println!("no more connection to accept");
                                break;
                            }
                            Err(e) => {
                                eprintln!("failed to accept connection with error {e}");
                                break;
                            }
                        }
                    }
                }
                other_token => {
                    if event.is_readable() {
                        let connection = connections
                            .get_mut(&other_token)
                            .expect("connection not presetn in connections");
                        let mut buf = [0u8; 4096];

                        match connection.stream.read(&mut buf) {
                            Ok(0) => {
                                poll.registry()
                                    .deregister(&mut connection.stream)
                                    .expect("could not deregister");

                                connection
                                    .stream
                                    .shutdown(std::net::Shutdown::Both)
                                    .expect("stream shut down failed");

                                let _ = connections
                                    .remove(&other_token)
                                    .expect("failed ot remove connection");
                                eprintln!("disconnected");
                            }
                            Ok(n) => {
                                connection.read_buf.extend(&buf[..n]);

                                let mut messages: Vec<u8> = Vec::new();
                                loop {
                                    let newline_pos =
                                        connection.read_buf.iter().rposition(|&b| b == b'\n');

                                    match newline_pos {
                                        Some(pos) => {
                                            let message: Vec<u8> =
                                                connection.read_buf.drain(..(pos + 1)).collect();
                                            messages.extend(message);
                                        }
                                        None => break,
                                    }
                                }

                                if messages.is_empty() {
                                    continue;
                                }

                                for (k, v) in connections.iter_mut() {
                                    if other_token == *k {
                                        continue;
                                    }
                                    v.write_buf.extend(&messages);

                                    poll.registry()
                                        .reregister(
                                            &mut v.stream,
                                            *k,
                                            Interest::READABLE | Interest::WRITABLE,
                                        )
                                        .expect("failed to reregister");
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                //Client would block we need to continue and try and read again
                                //later
                                eprintln!("would block stream read");
                                break;
                            }
                            Err(e) => {
                                eprintln!("gor error {e}");
                                break;
                            }
                        }
                    }

                    if event.is_writable() {
                        let connection = connections
                            .get_mut(&other_token)
                            .expect("connection not presetn in connections");
                        let n = connection
                            .stream
                            .write(connection.write_buf.make_contiguous())
                            .expect("failed to write");
                        for _ in 0..n {
                            connection.write_buf.pop_front();
                        }

                        poll.registry()
                            .reregister(&mut connection.stream, other_token, Interest::READABLE)
                            .expect("failed to reregister with read only");
                    }
                }
            }
        }
    }
}
