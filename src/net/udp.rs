use std::mem::MaybeUninit;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use socket2::SockAddr;
use socket2::{Socket, Domain, Type};

use pyo3::pyclass;
use pyo3::pymethods;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::common::{Trade, Order, AccountStatus, OrderSide, LogStatus};


#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    exchange: String,
    symbol: String,
    agent_id: String,
    msg: BroadcastMessageContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BroadcastMessageContent {
    trade(Trade),
    account(AccountStatus),
    order(Order),
}


pub struct UdbSender{
    socket: Socket,
    remote_addir: SockAddr
}

impl UdbSender {
    pub fn open(local_port: usize, remote_port: usize) -> Self {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();    

        let local_addr = format!("127.0.0.1:{}", local_port);
        let local_addr: SocketAddr = local_addr.parse().unwrap();
        socket.bind(&local_addr.into()).unwrap();

        let remote_addr = format!("127.0.0.1:{}", remote_port);
        let remote_addr: SocketAddr = remote_addr.parse().unwrap();
        let remote_addr = SockAddr::from(remote_addr);
    
        Self {
            socket: socket,
            remote_addir: remote_addr,
        }
    }

    pub fn send(&self, message: &str) -> Result<usize, std::io::Error> {
        self.socket.send_to(message.as_bytes(), &self.remote_addir)
    }

    pub fn send_message(&self, message: &BroadcastMessage) -> Result<usize, std::io::Error> {
        let msg = serde_json::to_string(message).unwrap();
        self.socket.send_to(msg.as_bytes(), &self.remote_addir)
    }
}

const UDP_SIZE: usize = 2048;

struct UdpReceiver {
    socket: Socket,
    local_addr: SockAddr,
    buf: [MaybeUninit<u8>; UDP_SIZE],
}

impl UdpReceiver {
    pub fn open(local_port: usize) -> Self {
        let local_addr = format!("127.0.0.1:{}", local_port);
        let local_addr: SocketAddr = local_addr.parse().unwrap();
 
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();    
        socket.set_reuse_address(true).unwrap();
        socket.set_reuse_port(true).unwrap();
    
        socket.bind(&local_addr.into()).unwrap();
        let buf = [MaybeUninit::uninit(); UDP_SIZE]; // Initialize the buffer with a properly sized array

        Self {
            socket: socket,
            local_addr: local_addr.into(),
            buf: buf,
        }
    }

    pub fn receive(&mut self) -> Result<String, std::io::Error> {
        let (amt, addr) = self.socket.recv_from(&mut self.buf)?;

        if let Some(sendr_ip) = addr.as_socket_ipv4() {
            if *(sendr_ip.ip()) != IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)) {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("invalid address 1 {:?}/{:?}", addr, self.local_addr)));                
            }
        }

        let msg = &self.buf[..amt];
        let m = unsafe { std::mem::transmute::<_, &[u8]>(msg) };
        
        let msg = std::str::from_utf8(m).unwrap();
        Ok(msg.to_string())
    }

    pub fn receive_message(&mut self) -> Result<BroadcastMessage, std::io::Error> {
        let msg = self.receive()?;
        let msg = serde_json::from_str::<BroadcastMessage>(&msg)?;
        Ok(msg)
    }
}    


fn broadcast(message: &str) {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();    

    let local_addr = "127.0.0.1:12450";
    let local_addr: SocketAddr = local_addr.parse().unwrap();
    socket.bind(&local_addr.into()).unwrap();

    let remote_addr = "127.0.0.1:12345";
    let remote_addr: SocketAddr = remote_addr.parse().unwrap();
    let remote_addr = SockAddr::from(remote_addr);

    socket.send_to(message.as_bytes(), &remote_addr).unwrap();
}



fn receive() -> String {
    const UDP_SIZE: usize = 2048;
        
    let local_addr = "127.0.0.1:12456";
    let local_addr: SocketAddr = local_addr.parse().unwrap();
 
    let mut socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();    
    socket.set_reuse_address(true).unwrap();
    socket.set_reuse_port(true).unwrap();

    socket.bind(&local_addr.into()).unwrap();

    let mut buf = [MaybeUninit::uninit(); UDP_SIZE]; // Initialize the buffer with a properly sized array
    let (amt, _) = socket.recv_from(&mut buf).unwrap();
    let msg = &buf[..amt];
    let m = unsafe { std::mem::transmute::<_, &[u8]>(msg) };
    
    let msg = std::str::from_utf8(m).unwrap();
    msg.to_string()
}


#[cfg(test)]
mod test_udp {
    #[test]
    fn send_test() {
        let msg = "hello world";
        super::broadcast(msg);
    }

    #[test]
    fn send_test2() {
        let sender = super::UdbSender::open(12450, 12345);
        sender.send("hello world").unwrap();
    }

    #[test]
    fn receive_test() {
        let msg = super::receive();
        println!("{}", msg);
    }

    #[test]
    fn receive_test2() {
        let mut receiver = super::UdpReceiver::open(12456);
        let msg = receiver.receive().unwrap();
        println!("{}", msg);
    }
}