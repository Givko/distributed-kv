use bitflags::bitflags;
use std::{collections::VecDeque, io};

pub enum State {
    //Listen,
    SynRcvd,
    Estab,
    FinWait1,
    FinWait2,
    TimeWait,
}

bitflags! {
    pub struct Available:u8 {
        const READ = 0b00000001;
        const WRITE = 0b00000010;
    }
}

///Send Sequence Space
///```
///           1         2          3          4
///      ----------|----------|----------|----------
///             SND.UNA    SND.NXT    SND.UNA
///                                  +SND.WND
///
///1 - old sequence numbers which have been acknowledged
///2 - sequence numbers of unacknowledged data
///3 - sequence numbers allowed for new data transmission
///4 - future sequence numbers which are not yet allowed
///```
pub struct SendSequenceSpace {
    una: u32,
    nxt: u32,
    wnd: u16,
    up: bool,
    wl1: u32,
    wl2: u32,
    iss: u32,
}

///Receive Sequence Space
///
///```
///               1          2          3
///           ----------|----------|----------
///                  RCV.NXT    RCV.NXT
///                            +RCV.WND
///
///1 - old sequence numbers which have been acknowledged
///2 - sequence numbers allowed for new reception
///3 - future sequence numbers which are not yet allowed
///```
pub struct RecvSequenceSpace {
    nxt: u32,
    wnd: u16,
    up: bool,
    irs: u32,
}

pub struct Connection {
    state: State,
    send: SendSequenceSpace,
    recv: RecvSequenceSpace,
    ip: etherparse::Ipv4Header,
    tcp: etherparse::TcpHeader,

    pub(crate) incoming: VecDeque<u8>,
    pub(crate) outgoing: VecDeque<u8>,
}

impl Connection {
    pub(crate) fn is_rcv_closed(&self) -> bool {
        if let State::TimeWait = self.state {
            //TODO: also close-wait, last-ack, closed, closing
            true
        } else {
            false
        }
    }
    fn availability(&self) -> Available {
        let mut a = Available::empty();
        if self.is_rcv_closed() || !self.incoming.is_empty() {
            a |= Available::READ;
        }
        a
    }

    pub fn on_packet<'a>(
        &mut self,
        nic: &mut tun_tap::Iface,
        iph: etherparse::Ipv4HeaderSlice<'a>,
        tcph: etherparse::TcpHeaderSlice<'a>,
        data: &'a [u8],
    ) -> io::Result<Available> {
        let seqn = tcph.sequence_number();
        let mut slen = data.len() as u32;
        if tcph.fin() {
            slen += 1;
        };
        if tcph.syn() {
            slen += 1;
        };
        let wend = self.recv.nxt.wrapping_add(self.recv.wnd as u32);
        let okay = if slen == 0 {
            if self.recv.wnd == 0 {
                seqn == self.recv.nxt
            } else {
                Connection::is_between_wrapped(self.recv.nxt.wrapping_sub(1), seqn, wend)
            }
        } else {
            self.recv.wnd > 0
                && (Connection::is_between_wrapped(self.recv.nxt.wrapping_sub(1), seqn, wend)
                    || Connection::is_between_wrapped(
                        self.recv.nxt.wrapping_sub(1),
                        seqn.wrapping_add(slen - 1),
                        wend,
                    ))
        };

        if !okay {
            self.write(nic, &[])?;
            return Ok(self.availability());
        }

        if !tcph.ack() {
            if tcph.syn() {
                assert!(data.is_empty());
                self.recv.nxt = seqn.wrapping_add(1);
            }
            return Ok(self.availability());
        }

        let ackn = tcph.acknowledgment_number();
        if let State::SynRcvd = self.state {
            if Connection::is_between_wrapped(
                self.send.una.wrapping_sub(1),
                ackn,
                self.send.nxt.wrapping_add(1),
            ) {
                // must have ACKed our SYN, since we detected at least one acked byte,
                // and we have only sent one byte (the SYN).
                self.state = State::Estab;
            } else {
                // TODO: <SEQ=SEG.ACK><CTL=RST>
            }
        }

        if let State::Estab | State::FinWait1 | State::FinWait2 = self.state {
            if Connection::is_between_wrapped(self.send.una, ackn, self.send.nxt.wrapping_add(1)) {
                self.send.una = ackn;
            }
            // TODO
            //assert!(data.is_empty());

            if let State::Estab = self.state {
                // now let's terminate the connection!
                // TODO: needs to be stored in the retransmission queue!
                self.tcp.fin = true;
                //     self.write(nic, &[])?;
                self.state = State::FinWait1;
            }
        }

        if let State::FinWait1 = self.state {
            if self.send.una == self.send.iss + 2 {
                // our FIN has been ACKed!
                self.state = State::FinWait2;
            }
        }

        if let State::Estab | State::FinWait1 | State::FinWait2 = self.state {
            let mut unread_data_at = (self.recv.nxt - seqn) as usize;
            if unread_data_at > data.len() {
                assert_eq!(unread_data_at, data.len() + 1);
                unread_data_at = 0;
            }

            self.incoming.extend(&data[unread_data_at..]);
            self.recv.nxt = seqn
                .wrapping_add(if tcph.fin() { 1 } else { 0 })
                .wrapping_add(data.len() as u32);

            eprintln!("send ack for data");
            self.write(nic, &[])?;
        }

        if tcph.fin() {
            match self.state {
                State::FinWait2 => {
                    // we're done with the connection!
                    self.write(nic, &[])?;
                    self.state = State::TimeWait;
                }
                _ => unimplemented!(),
            }
        }

        Ok(self.availability())
    }

    fn wrapping_lt(lhs: u32, rhs: u32) -> bool {
        // From RFC1323:
        //     TCP determines if a data segment is "old" or "new" by testing
        //     whether its sequence number is within 2**31 bytes of the left edge
        //     of the window, and if it is not, discarding the data as "old".  To
        //     insure that new data is never mistakenly considered old and vice-
        //     versa, the left edge of the sender's window has to be at most
        //     2**31 away from the right edge of the receiver's window.
        lhs.wrapping_sub(rhs) > 2 ^ 31
    }

    fn is_between_wrapped(start: u32, x: u32, end: u32) -> bool {
        Connection::wrapping_lt(start, x) && Connection::wrapping_lt(x, end)
    }
    pub fn accept<'a>(
        nic: &mut tun_tap::Iface,
        iph: etherparse::Ipv4HeaderSlice<'a>,
        tcph: etherparse::TcpHeaderSlice<'a>,
        data: &'a [u8],
    ) -> io::Result<Option<Self>> {
        if !tcph.syn() {
            // only expected SYN packet
            return Ok(None);
        }

        let iss = 0;
        let wnd = 1024;
        let mut c = Connection {
            incoming: VecDeque::new(),
            outgoing: VecDeque::new(),
            state: State::SynRcvd,
            send: SendSequenceSpace {
                iss,
                una: iss,
                nxt: iss,
                wnd,
                up: false,

                wl1: 0,
                wl2: 0,
            },
            recv: RecvSequenceSpace {
                irs: tcph.sequence_number(),
                nxt: tcph.sequence_number() + 1,
                wnd: tcph.window_size(),
                up: false,
            },
            tcp: etherparse::TcpHeader::new(tcph.destination_port(), tcph.source_port(), iss, wnd),
            ip: etherparse::Ipv4Header::new(
                0,
                64,
                etherparse::IpNumber::TCP,
                iph.destination_addr().octets(),
                iph.source_addr().octets(),
            )
            .expect("false to create ip header"),
        };

        // need to start establishing a connection
        c.tcp.syn = true;
        c.tcp.ack = true;
        c.write(nic, &[])?;
        Ok(Some(c))
    }

    fn write(&mut self, nic: &mut tun_tap::Iface, payload: &[u8]) -> io::Result<usize> {
        let mut buf = [0u8; 1500];
        self.tcp.sequence_number = self.send.nxt;
        self.tcp.acknowledgment_number = self.recv.nxt;

        let size = std::cmp::min(
            buf.len(),
            self.tcp.header_len() + self.ip.header_len() + payload.len(),
        );
        _ = self.ip.set_payload_len(size - self.ip.header_len());

        // the kernel is nice and does this for us
        self.tcp.checksum = self
            .tcp
            .calc_checksum_ipv4(&self.ip, &[])
            .expect("failed to compute checksum");

        // write out the headers
        use std::io::Write;
        let mut unwritten = &mut buf[..];
        _ = self.ip.write(&mut unwritten);
        _ = self.tcp.write(&mut unwritten);
        let payload_bytes = unwritten.write(payload)?;
        let unwritten = unwritten.len();
        self.send.nxt = self.send.nxt.wrapping_add(payload_bytes as u32);
        if self.tcp.syn {
            self.send.nxt = self.send.nxt.wrapping_add(1);
            self.tcp.syn = false;
        }
        if self.tcp.fin {
            self.send.nxt = self.send.nxt.wrapping_add(1);
            self.tcp.fin = false;
        }
        nic.send(&buf[..buf.len() - unwritten])?;
        Ok(payload_bytes)
    }

    fn send_rst(&mut self, nic: &mut tun_tap::Iface) -> io::Result<()> {
        self.tcp.rst = true;
        // TODO: fix sequence numbers here
        // If the incoming segment has an ACK field, the reset takes its
        // sequence number from the ACK field of the segment, otherwise the
        // reset has sequence number zero and the ACK field is set to the sum
        // of the sequence number and segment length of the incoming segment.
        // The connection remains in the same state.
        //
        // TODO: handle synchronized RST
        // 3.  If the connection is in a synchronized state (ESTABLISHED,
        // FIN-WAIT-1, FIN-WAIT-2, CLOSE-WAIT, CLOSING, LAST-ACK, TIME-WAIT),
        // any unacceptable segment (out of window sequence number or
        // unacceptible acknowledgment number) must elicit only an empty
        // acknowledgment segment containing the current send-sequence number
        // and an acknowledgment indicating the next sequence number expected
        // to be received, and the connection remains in the same state.
        self.tcp.sequence_number = 0;
        self.tcp.acknowledgment_number = 0;
        self.write(nic, &[])?;
        Ok(())
    }
}
