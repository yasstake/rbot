use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;

use crossbeam_channel::Receiver;
use socket2::{SockAddr, Protocol};
use socket2::{Domain, Socket, Type};

use pyo3::pyclass;
use pyo3::pymethods;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use crate::common::{AccountStatus, Order, Trade};
use crate::{MarketMessage, env_rbot_multicast_addr, env_rbot_multicast_port};
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
    multicast_addr: SockAddr,
}

#[pymethods]
impl UdpSender {
    #[staticmethod]
    pub fn open(
        market_name: &str,
        market_category: &str,
        symbol: &str,
    ) -> Self {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
        socket.set_reuse_address(true).unwrap();
        socket.set_reuse_port(true).unwrap();

        let multicast_addr = format!("{}:{}", env_rbot_multicast_addr(), env_rbot_multicast_port());

        let multicast_addr: SocketAddr = multicast_addr.parse().unwrap();

        Self {
            exchange_name: market_name.to_string(),
            category: market_category.to_string(),
            symbol: symbol.to_string(),
            socket: socket,
            multicast_addr: multicast_addr.into(),
        }
    }

    pub fn send(&self, message: &str) -> Result<usize, std::io::Error> {
        log::debug!("UDP send: [{:?}], {}", &self.multicast_addr, message);
        self.socket.send_to(message.as_bytes(), &self.multicast_addr)
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
                msg: BroadcastMessageContent::trade(trade.clone()),
            },
            MarketMessage {
                order: Some(order), ..
            } => BroadcastMessage {
                exchange: exchange,
                category: category,
                symbol: symbol,
                msg: BroadcastMessageContent::order(order.clone()),
            },
            MarketMessage {
                account: Some(account),
                ..
            } => BroadcastMessage {
                exchange: exchange,
                category: category,
                symbol: symbol,
                msg: BroadcastMessageContent::account(account.clone()),
            },
            _ => {
                panic!("Unknown message type {:?}", message);
            }
        };

        let msg = serde_json::to_string(&message).unwrap();

        log::debug!("send:[{:?}] {}", &self.multicast_addr, msg);

        self.socket.send_to(msg.as_bytes(), &self.multicast_addr)
    }

    pub fn send_message(&self, message: &BroadcastMessage) -> Result<usize, std::io::Error> {
        let msg = serde_json::to_string(message).unwrap();
        self.socket.send_to(msg.as_bytes(), &self.multicast_addr)
    }
}

const UDP_SIZE: usize = 4096;

#[derive(Debug)]

pub struct UdpReceiver {
    market_name: String,
    market_category: String,
    symbol: String,
    socket: Socket,
    buf: [MaybeUninit<u8>; UDP_SIZE],
}

impl UdpReceiver {
    pub fn open(market_name: &str, market_category: &str, symbol: &str, agent_id: &str) -> Self {
        let multicast_addr = Ipv4Addr::from_str(&env_rbot_multicast_addr());
        if multicast_addr.is_err() {
            log::error!("multicast_addr error {:?}", multicast_addr);
        }
        let multicast_addr = multicast_addr.unwrap();
        let multicast_port = env_rbot_multicast_port();

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
        socket.set_reuse_address(true).unwrap();
        socket.set_reuse_port(true).unwrap();

        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, multicast_port as u16);
        let addr = SockAddr::from(addr);
        let r = socket.bind(&addr);
        if r.is_err() {
            log::error!("bind error");
        }
        
        let r = socket.join_multicast_v4(&multicast_addr, &Ipv4Addr::UNSPECIFIED);
        if r.is_err() {
            log::error!("join_multicast_v4 error");
        }

        let buf = [MaybeUninit::uninit(); UDP_SIZE]; // Initialize the buffer with a properly sized array

        Self {
            market_name: market_name.to_string(),
            market_category: market_category.to_string(),
            symbol: symbol.to_string(),
            socket: socket,
            buf: buf,
        }
    }

    pub fn receive(&mut self) -> Result<String, std::io::Error> {
        let (amt, addr) = self.socket.recv_from(&mut self.buf)?;

        /*
        if let Some(sendr_ip) = addr.as_socket_ipv4() {
            if *(sendr_ip.ip()) != IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("invalid address 1 {:?}/{:?}", addr, self.local_addr),
                ));
            }
        }
        */

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

            log::debug!("receive_market_message raw: {:?}", msg);

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

    pub fn open_channel(market_name: &str, market_category: &str, symbol: &str, agent_id: &str) -> Result<Receiver<MarketMessage>, std::io::Error> {
        let mut udp = Self::open(market_name, market_category, symbol, agent_id);
        let (tx, rx) = crossbeam_channel::unbounded();
        
        std::thread::spawn(move || loop {
            let msg = udp.receive_market_message().unwrap();

            let r = tx.send(msg.clone());
            
            if r.is_err() {
                log::error!("open_channel: {}/{:?}", r.err().unwrap(), msg);
                break;
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod test_udp {
    use crate::common::init_debug_log;

    #[test]
    fn send_test2() {
        let sender = super::UdpSender::open("EXA", "linear", "BCTUSD");
        sender.send("hello world").unwrap();
    }

    #[test]
    fn receive_test2() {
        init_debug_log();        
        let mut receiver = super::UdpReceiver::open("EXA", "linear", "BTCUSDT", "x");
        let msg = receiver.receive().unwrap();
        println!("{}", msg);
    }

    #[test]
    fn receive_test3() {
        init_debug_log();
        let mut receiver = super::UdpReceiver::open("EXA", "linear", "BTCUSDT", "b");

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
