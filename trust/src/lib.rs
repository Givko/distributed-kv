use etherparse::{IpNumber, TcpHeaderSlice};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::Ipv4Addr;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

mod tcp;
const SEND_QUEUE_SIZE: usize = 1024;
type InterfaceHandle = Arc<Synchronizer>;

#[derive(Default)]
struct Synchronizer {
    manager: Mutex<ConnectionManager>,
    pending_var: Condvar,
    recv_var: Condvar,
}

pub struct Interface {
    ih: Option<InterfaceHandle>,
    jh: Option<thread::JoinHandle<io::Result<()>>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Quad {
    src: (Ipv4Addr, u16),
    dst: (Ipv4Addr, u16),
}

#[derive(Default)]
struct ConnectionManager {
    connections: HashMap<Quad, tcp::Connection>,
    pending: HashMap<u16, VecDeque<Quad>>,
}

impl Drop for Interface {
    fn drop(&mut self) {
        drop(self.ih.take());
        self.jh
            .take()
            .expect("interface dropped more than once")
            .join()
            .unwrap()
            .unwrap();
    }
}

fn packet_run(mut nic: tun_tap::Iface, ih: InterfaceHandle) -> io::Result<()> {
    let mut buf = [0u8; 1504];
    loop {
        let nbytes = nic.recv(&mut buf[..])?;
        match etherparse::Ipv4HeaderSlice::from_slice(&buf[..nbytes]) {
            Ok(iph) => {
                let src = iph.source_addr();
                let dst = iph.destination_addr();
                if iph.protocol() != IpNumber::TCP {
                    continue;
                }

                let ip_hdr_size = iph.slice().len();
                match TcpHeaderSlice::from_slice(&buf[ip_hdr_size..nbytes]) {
                    Ok(tcp) => {
                        let tcp_hdr_size = tcp.slice().len();
                        let data_index = ip_hdr_size + tcp_hdr_size;
                        let mut cmg = ih.manager.lock().unwrap();
                        let cm = &mut *cmg;

                        let quad = Quad {
                            src: (src, tcp.source_port()),
                            dst: (dst, tcp.destination_port()),
                        };
                        match cm.connections.entry(quad) {
                            Entry::Occupied(mut c) => {
                                let a = c.get_mut().on_packet(
                                    &mut nic,
                                    iph,
                                    tcp,
                                    &buf[data_index..nbytes],
                                )?;
                                if a.contains(tcp::Available::READ) {
                                    ih.recv_var.notify_all();
                                }
                            }
                            Entry::Vacant(e) => {
                                if let Some(pending) = cm.pending.get_mut(&tcp.destination_port()) {
                                    if let Some(c) = tcp::Connection::accept(
                                        &mut nic,
                                        iph,
                                        tcp,
                                        &buf[data_index..nbytes],
                                    )? {
                                        e.insert(c);
                                        pending.push_back(quad);

                                        ih.pending_var.notify_all();
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        eprintln!("ignore wierd tcp header")
                    }
                }
            }
            Err(_) => {
                //eprintln!("ignore wierd packet")
            }
        }
    }
}
impl Interface {
    pub fn new() -> io::Result<Self> {
        let iface = tun_tap::Iface::without_packet_info("tun0", tun_tap::Mode::Tun)?;
        let ih: InterfaceHandle = Arc::default();

        let jh = {
            let ih = ih.clone();
            thread::spawn(move || packet_run(iface, ih))
        };

        Ok(Interface {
            ih: Some(ih),
            jh: Some(jh),
        })
    }

    pub fn bind(&mut self, port: u16) -> io::Result<TcpListener> {
        let mut cm = self.ih.as_mut().unwrap().manager.lock().unwrap();
        //TODO: something to start accepting SYN packets on port
        match cm.pending.entry(port) {
            Entry::Vacant(v) => {
                v.insert(VecDeque::new());
            }
            Entry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "the port is occupied",
                ));
            }
        }
        drop(cm);

        Ok(TcpListener {
            port,
            handle: self.ih.as_mut().unwrap().clone(),
        })
    }
}

pub struct TcpListener {
    port: u16,
    handle: InterfaceHandle,
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        let mut cm = self.handle.manager.lock().unwrap();
        let pending = cm
            .pending
            .remove(&self.port)
            .expect("port closed while listener still active");
        for q in pending {
            //TODO: terminate pending connections for quad
            unimplemented!();
        }
    }
}

impl TcpListener {
    pub fn accept(&mut self) -> std::io::Result<TcpStream> {
        let mut cm = self.handle.manager.lock().unwrap();
        loop {
            if let Some(quad) = cm
                .pending
                .get_mut(&self.port)
                .expect("port closed while listener still active")
                .pop_front()
            {
                return Ok(TcpStream {
                    quad,
                    handle: self.handle.clone(),
                });
            }

            cm = self.handle.pending_var.wait(cm).unwrap();
        }
    }
}

pub struct TcpStream {
    quad: Quad,
    handle: InterfaceHandle,
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let mut cm = self.handle.manager.lock().unwrap();
        //if let Some(c) = cm.connections.remove(&self.quad) {
        //TODO: Send FIN for cm.connections[quad]
        //unimplemented!()
        //}
    }
}

impl Write for TcpStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut cm = self.handle.manager.lock().unwrap();
        let c = cm.connections.get_mut(&self.quad).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "stream was terminated unexpectedly",
            )
        })?;
        if c.outgoing.len() >= SEND_QUEUE_SIZE {
            //TODO: block until space is available
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "would block until space in queue is available",
            ));
        }
        let nbytes = std::cmp::min(buf.len(), SEND_QUEUE_SIZE - c.outgoing.len());
        c.outgoing.extend(&buf[..nbytes]);
        //TODO: Wake up writers
        Ok(nbytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut cm = self.handle.manager.lock().unwrap();
        let c = cm.connections.get_mut(&self.quad).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "stream was terminated unexpectedly",
            )
        })?;
        if c.outgoing.is_empty() {
            Ok(())
        } else {
            //TODO: block until outgoing is empty
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "too many bytes buffered",
            ))
        }
    }
}
impl Read for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut cm = self.handle.manager.lock().unwrap();
        loop {
            let c = cm.connections.get_mut(&self.quad).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "stream was terminated unexpectedly",
                )
            })?;

            if c.is_rcv_closed() && c.incoming.is_empty() {
                eprintln!("no data");
                return Ok(0);
            }
            if !c.incoming.is_empty() {
                let mut nread = 0;
                let (head, tail) = c.incoming.as_slices();
                let hread = std::cmp::min(buf.len(), head.len());
                buf[..hread].copy_from_slice(&head[..hread]);
                nread += hread;
                let tread = std::cmp::min(buf.len() - nread, tail.len());
                buf[hread..(hread + tread)].copy_from_slice(&tail[..tread]);
                nread += tread;
                drop(c.incoming.drain(..nread));
                return Ok(nread);
            }

            //if !c.incoming.is_empty() {
            //    //TODO: detect FIN and return nread == 0
            //    c.incoming.make_contiguous();
            //    let (slice, _) = c.incoming.as_slices();
            //    let nread = std::cmp::min(buf.len(), slice.len());
            //    buf[..nread].copy_from_slice(&slice[..nread]);
            //    drop(c.incoming.drain(..nread));
            //    eprintln!("drained data from incoming {:?}", c.incoming.len());
            //    eprintln!("read data {nread}");
            //    return Ok(nread);
            //}

            cm = self.handle.recv_var.wait(cm).unwrap();
        }
    }
}
