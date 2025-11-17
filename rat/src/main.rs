use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::time::Duration;

struct ClientConnection {
    stream: TcpStream,
    read_buf: VecDeque<u8>,
    write_buf: VecDeque<u8>,
}

fn main() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let mut listener =
        TcpListener::bind("127.0.0.1:8080".parse().unwrap()).expect("Unable to bind");
    let listener_token = Token(0);

    poll.registry()
        .register(&mut listener, listener_token, Interest::READABLE)
        .expect("Failed to register listener");

    let mut connections: HashMap<Token, ClientConnection> = HashMap::new();
    let mut next_token_id = 1;
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100)))
            .expect("Failed to poll");

        for event in events.iter() {
            match event.token() {
                Token(0) => {
                    // Listener - accept connections
                    if !event.is_readable() {
                        continue;
                    }

                    loop {
                        match listener.accept() {
                            Ok((mut stream, addr)) => {
                                println!("Accepted connection from {}", addr);

                                let stream_token = Token(next_token_id);
                                next_token_id += 1;

                                poll.registry()
                                    .register(&mut stream, stream_token, Interest::READABLE)
                                    .expect("Failed to register stream");

                                let connection = ClientConnection {
                                    stream,
                                    read_buf: VecDeque::new(),
                                    write_buf: VecDeque::new(),
                                };
                                connections.insert(stream_token, connection);

                                println!("Registered client with {:?}", stream_token);
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                break;
                            }
                            Err(e) => {
                                eprintln!("Failed to accept: {}", e);
                                break;
                            }
                        }
                    }
                }
                other_token => {
                    // Client event
                    if event.is_readable() {
                        let connection = connections
                            .get_mut(&other_token)
                            .expect("Connection not found");

                        // Read loop (edge-triggered)
                        let mut messages: Vec<u8> = Vec::new();
                        loop {
                            let mut buf = [0u8; 4096];

                            match connection.stream.read(&mut buf) {
                                Ok(0) => {
                                    // Connection closed
                                    println!("Client {:?} disconnected", other_token);

                                    poll.registry()
                                        .deregister(&mut connection.stream)
                                        .expect("Failed to deregister");

                                    connections.remove(&other_token);
                                    break;
                                }
                                Ok(n) => {
                                    // Append newly read bytes
                                    connection.read_buf.extend(&buf[..n]);

                                    // Find last newline
                                    let last_newline =
                                        connection.read_buf.iter().rposition(|&b| b == b'\n');
                                    match last_newline {
                                        Some(pos) => {
                                            // Extract all complete messages
                                            let message: Vec<u8> =
                                                connection.read_buf.drain(..(pos + 1)).collect();
                                            messages.extend(message);

                                            // Broadcast to all other clients
                                            println!(
                                                "Broadcast {} bytes from {:?}",
                                                messages.len(),
                                                other_token,
                                            );
                                        }
                                        None => {
                                            println!(
                                                "Buffering partial message from {:?}",
                                                other_token
                                            );
                                        }
                                    }
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("Read error: {}", e);
                                    break;
                                }
                            }
                        }

                        if messages.is_empty() {
                            continue;
                        }

                        for (token, conn) in connections.iter_mut() {
                            if *token == other_token {
                                continue;
                            }

                            conn.write_buf.extend(&messages);

                            poll.registry()
                                .reregister(
                                    &mut conn.stream,
                                    *token,
                                    Interest::READABLE | Interest::WRITABLE,
                                )
                                .expect("Failed to reregister");
                        }
                    }
                    if event.is_writable() {
                        let connection = connections
                            .get_mut(&other_token)
                            .expect("Connection not found");

                        // Write loop (edge-triggered - drain until WouldBlock)
                        loop {
                            if connection.write_buf.is_empty() {
                                // Nothing left to write - reregister for READABLE only
                                poll.registry()
                                    .reregister(
                                        &mut connection.stream,
                                        other_token,
                                        Interest::READABLE,
                                    )
                                    .expect("Failed to reregister");
                                break;
                            }

                            // Get contiguous slice of write buffer
                            let buf = connection.write_buf.make_contiguous();

                            match connection.stream.write(buf) {
                                Ok(0) => {
                                    // Socket closed
                                    println!("Client {:?} closed during write", other_token);

                                    poll.registry()
                                        .deregister(&mut connection.stream)
                                        .expect("Failed to deregister");

                                    connections.remove(&other_token);
                                    break;
                                }
                                Ok(n) => {
                                    // Wrote n bytes - remove from buffer
                                    println!("Wrote {} bytes to {:?}", n, other_token);

                                    connection.write_buf.drain(..n);
                                    // Continue loop - might have more to write
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    // Socket not ready - will get another WRITABLE event
                                    println!("Write would block for {:?}", other_token);
                                    break;
                                }
                                Err(e) => {
                                    eprintln!("Write error for {:?}: {}", other_token, e);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
