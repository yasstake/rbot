use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use socket2::SockAddr;
use socket2::{Domain, Socket, Type};

use pyo3::pyclass;
use pyo3::pymethods;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::common::{AccountStatus, Order, Trade};
use crate::MarketMessage;
use crate::exchange::bitflyer::market;

use super::{get_udp_port, get_udp_source_port};

/// TODO: BroadcastMessageにliniiearの種別を加える
/// TODO: Sender,Receiverを実装する。

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub exchange: String,
    pub category: String,
    pub symbol: String,
    pub agent_id: String,
    pub msg: BroadcastMessageContent,
}

impl Into<MarketMessage> for BroadcastMessage {
    fn into(self) -> MarketMessage {
        let msg = match self.msg {
            BroadcastMessageContent::trade(trade) => MarketMessage::from_trade(trade),
            BroadcastMessageContent::order(order) => MarketMessage::from_order(order),
            BroadcastMessageContent::account(account) => MarketMessage::from_account(account),
        };
        msg
    }
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BroadcastMessageContent {
    trade(Trade),
    account(AccountStatus),
    order(Order),
}

#[derive(Debug)]
#[pyclass]
pub struct UdpSender {
    exchange_name: String,
    category: String,
    symbol: String,
    socket: Socket,
    remote_addr: SockAddr,
}

#[pymethods]
impl UdpSender {
    #[staticmethod]
    pub fn open(market_name: &str, market_category: &str, symbol: &str) -> Self {
        let udp_port = get_udp_port();
        let udp_source_port = get_udp_source_port();

        Self::open_with_port(
            market_name,
            market_category,
            symbol,
            udp_source_port,
            udp_port,
        )
    }

    #[staticmethod]
    pub fn open_with_port(
        market_name: &str,
        market_category: &str,
        symbol: &str,
        local_port: i64,
        remote_port: i64,
    ) -> Self {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();

        for i in 0..100 {
            let port = local_port + i;
            if port == remote_port {
                continue;
            }

            let local_addr = format!("127.0.0.1:{}", port);
            let local_addr: SocketAddr = local_addr.parse().unwrap();

            if socket.bind(&local_addr.into()).is_ok() {
                break;
            }
        }

        log::debug!(
            "open_with_port: {} / {} local={}, remote={}",
            market_name,
            symbol,
            local_port,
            remote_port
        );

        let remote_addr = format!("127.0.0.1:{}", remote_port);
        let remote_addr: SocketAddr = remote_addr.parse().unwrap();

        Self {
            exchange_name: market_name.to_string(),
            category: market_category.to_string(),
            symbol: symbol.to_string(),
            socket: socket,
            remote_addr: SockAddr::from(remote_addr),
        }
    }

    pub fn send(&self, message: &str) -> Result<usize, std::io::Error> {
        self.socket.send_to(message.as_bytes(), &self.remote_addr)
    }

    pub fn send_market_message(&self, message: &MarketMessage) -> Result<usize, std::io::Error> {
        let exchange = self.exchange_name.clone();
        let category = self.category.clone();
        let symbol = self.symbol.clone();
        let agent_id = "".to_string();

        let message = match message {
            MarketMessage {
                trade: Some(trade), ..
            } => BroadcastMessage {
                exchange: exchange,
                category: category,
                symbol: symbol,
                agent_id: agent_id,
                msg: BroadcastMessageContent::trade(trade.clone()),
            },
            MarketMessage {
                order: Some(order), ..
            } => BroadcastMessage {
                exchange: exchange,
                category: category,
                symbol: symbol,
                agent_id: agent_id,
                msg: BroadcastMessageContent::order(order.clone()),
            },
            MarketMessage {
                account: Some(account),
                ..
            } => BroadcastMessage {
                exchange: exchange,
                category: category,
                symbol: symbol,
                agent_id: agent_id,
                msg: BroadcastMessageContent::account(account.clone()),
            },
            _ => {
                panic!("Unknown message type {:?}", message);
            }
        };

        let msg = serde_json::to_string(&message).unwrap();

        log::debug!("send_market_message: {}", msg);

        self.socket.send_to(msg.as_bytes(), &self.remote_addr)
    }

    pub fn send_message(&self, message: &BroadcastMessage) -> Result<usize, std::io::Error> {
        let msg = serde_json::to_string(message).unwrap();
        self.socket.send_to(msg.as_bytes(), &self.remote_addr)
    }
}

const UDP_SIZE: usize = 4096;

#[derive(Debug)]
#[pyclass]
pub struct UdpReceiver {
    market_name: String,
    market_category: String,
    symbol: String,
    socket: Socket,
    local_addr: SockAddr,
    buf: [MaybeUninit<u8>; UDP_SIZE],
}

#[pymethods]
impl UdpReceiver {
    #[staticmethod]
    pub fn open(market_name: &str, market_category: &str, symbol: &str) -> Self {
        let udp_port = get_udp_port();
        Self::open_with_port(market_name, market_category, symbol, udp_port)
    }

    #[staticmethod]
    pub fn open_with_port(market_name: &str, market_category: &str, symbol: &str, local_port: i64) -> Self {
        let local_addr = format!("127.0.0.1:{}", local_port);
        let local_addr: SocketAddr = local_addr.parse().unwrap();

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();
        socket.set_reuse_address(true).unwrap();
        socket.set_reuse_port(true).unwrap();

        socket.bind(&local_addr.into()).unwrap();
        let buf = [MaybeUninit::uninit(); UDP_SIZE]; // Initialize the buffer with a properly sized array

        Self {
            market_name: market_name.to_string(),
            market_category: market_category.to_string(),
            symbol: symbol.to_string(),
            socket: socket,
            local_addr: local_addr.into(),
            buf: buf,
        }
    }

    pub fn receive(&mut self) -> Result<String, std::io::Error> {
        let (amt, addr) = self.socket.recv_from(&mut self.buf)?;

        if let Some(sendr_ip) = addr.as_socket_ipv4() {
            if *(sendr_ip.ip()) != IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("invalid address 1 {:?}/{:?}", addr, self.local_addr),
                ));
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

    pub fn receive_market_message(&mut self) -> Result<MarketMessage, std::io::Error> {
        let mut msg: BroadcastMessage;

        loop {
            msg = self.receive_message()?;

            if (msg.exchange == self.market_name || self.market_name == "")
                && (msg.category == self.market_category || self.market_category == "")
                && (msg.symbol == self.symbol || self.symbol == "")
            {
                break;
            }
        }

        let market_message: MarketMessage = msg.into();
        Ok(market_message)
    }
}

#[cfg(test)]
mod test_udp {
    #[test]
    fn send_test2() {
        let sender = super::UdpSender::open("EXA", "linear", "BCTUSD");
        sender.send("hello world").unwrap();
    }

    #[test]
    fn receive_test2() {
        let mut receiver = super::UdpReceiver::open("EXA", "linear", "BTCUSDT");
        let msg = receiver.receive().unwrap();
        println!("{}", msg);
    }

    #[test]
    fn receive_test3() {
        let mut receiver = super::UdpReceiver::open("EXA", "linear", "BTCUSDT");

        let mut count = 100;

        loop {
            let msg = receiver.receive_message().unwrap();
            println!("{:?}", msg);

            if count == 0 {
                break;
            }
            count -= 1;
        }
    }
}
