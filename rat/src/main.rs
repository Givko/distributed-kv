use std::collections::VecDeque;
use std::vec::Vec;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::broadcast::{Receiver, Sender, error::RecvError},
};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .expect("Unable to bind");
    let (snd, _) = tokio::sync::broadcast::channel::<Vec<u8>>(256);
    loop {
        let (stream, _) = listener.accept().await.expect("unable to accept");
        //connections.insert(stream_token, connection);

        eprintln!("accepted connetion");
        let (r, w) = stream.into_split();
        tokio::spawn(read(r, snd.clone()));
        tokio::spawn(write(w, snd.subscribe()));
    }
}

async fn write(mut write_stream: OwnedWriteHalf, mut rcv: Receiver<Vec<u8>>) {
    let mut write_buf: VecDeque<u8> = VecDeque::new();
    loop {
        match rcv.recv().await {
            Ok(bytes) => {
                write_buf.extend(&bytes);

                // Get contiguous slice of write buffer
                let buf = write_buf.make_contiguous();
                match write_stream.write(buf).await {
                    Ok(0) => {
                        break;
                    }
                    Ok(n) => {
                        // Wrote n bytes - remove from buffer
                        println!("Wrote {} bytes to", n);

                        write_buf.drain(..n);
                        // Continue loop - might have more to write
                    }
                    Err(_) => {
                        break;
                    }
                }
                eprintln!("Recieved {} bytes", bytes.len());
            }
            Err(RecvError::Closed) => {
                eprintln!("Sender close");
                break;
            }
            Err(_) => {
                eprintln!("Failed unexpectedly");
            }
        }
    }
}

async fn read(mut read_stream: OwnedReadHalf, sender: Sender<Vec<u8>>) {
    println!("started reading");
    let mut read_buf = VecDeque::new();
    let mut messages: Vec<u8> = Vec::new();
    loop {
        let mut buf = [0u8; 1024];
        let n = read_stream.read(&mut buf).await.expect("Unable to read");

        // 0 bytes means connection closed
        if n == 0 {
            println!("Closed connection");
            break;
        }

        // Append newly read bytes
        read_buf.extend(&buf[..n]);
        eprintln!("Read {} bytes", n);

        // Find last newline
        let last_newline = read_buf.iter().rposition(|&b| b == b'\n');
        match last_newline {
            Some(pos) => {
                // Extract all complete messages
                let message: Vec<u8> = read_buf.drain(..(pos + 1)).collect();
                messages.extend(message);

                // Broadcast to all other clients
                println!("Broadcast {} bytes from {:?}", messages.len(), n);
            }
            None => {
                println!("Buffering partial message from ");
            }
        }

        if messages.is_empty() {
            continue;
        }

        sender
            .send(messages.clone())
            .expect("Unable to send messages");

        eprintln!("Send all message bytes {}", messages.len());
        messages.clear();
    }
}
