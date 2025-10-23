use std::io::{self, Read};
use std::thread;

fn main() -> io::Result<()> {
    let mut i = trust::Interface::new()?;
    let mut l2 = i.bind(8000)?;

    let jh2 = thread::spawn(move || {
        while let Ok(mut stream) = l2.accept() {
            eprintln!("got connection");
            loop {
                let mut buf = [0; 512];
                let n = stream.read(&mut buf[..]).unwrap();
                if n == 0 {
                    eprintln!("no more data!");
                    break;
                } else {
                    println!("{}", std::str::from_utf8(&buf[..n]).unwrap());
                }
            }
        }
    });
    _ = jh2.join().unwrap();
    Ok(())
}
