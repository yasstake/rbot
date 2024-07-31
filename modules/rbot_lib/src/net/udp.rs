use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;

use futures::Stream;

use crossbeam_channel::Receiver;
use socket2::{Domain, Socket, Type};
use socket2::{Protocol, SockAddr};

use pyo3::pyclass;
// use pyo3::pymethods;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use async_stream::stream;

use crate::common::{env_rbot_multicast_addr, env_rbot_multicast_port, MarketMessage};

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub exchange: String,
    pub category: String,
    pub symbol: String,
    pub msg: MarketMessage,
}

impl BroadcastMessage {
    pub fn filter(&self, exchange: &str, category: &str, symbol: &str) -> bool {
        (self.exchange == exchange || exchange == "")
            && (self.category == category || category == "")
            && (self.symbol == symbol || symbol == "")
    }
}

impl Into<MarketMessage> for BroadcastMessage {
    fn into(self) -> MarketMessage {
        self.msg
    }
}

#[derive(Debug)]
pub struct UdpSender {
    socket: Socket,
    multicast_addr: SockAddr,
}

impl UdpSender {
    pub fn open() -> Self {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();

        // Windowsの場合
        #[cfg(target_os = "windows")]
        {
            socket.set_reuse_address(true).unwrap();
            // socket.set_exclusive_address_use(false).unwrap();
        }

        // Windows以外の場合
        #[cfg(not(target_os = "windows"))]
        {
            socket.set_reuse_address(true).unwrap();
            // SO_REUSEPORTはLinuxとmacOSで利用可能ですが、Windowsでは利用できません。
            // そのため、このオプションを設定するコードはWindows以外でのみコンパイルされます。
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            socket.set_reuse_port(true).unwrap();
        }

        let multicast_addr = format!(
            "{}:{}",
            env_rbot_multicast_addr(),
            env_rbot_multicast_port()
        );

        let multicast_addr: SocketAddr = multicast_addr.parse().unwrap();

        Self {
            socket: socket,
            multicast_addr: multicast_addr.into(),
        }
    }

    pub fn send(&self, message: &str) -> Result<usize, std::io::Error> {
        log::debug!("UDP send: [{:?}], {}", &self.multicast_addr, message);
        self.socket
            .send_to(message.as_bytes(), &self.multicast_addr)
    }

    pub fn send_market_message(
        &self,
        exchange_name: &str,
        category: &str,
        symbol: &str,
        message: &MarketMessage,
    ) -> anyhow::Result<usize> {
        let exchange = exchange_name.to_string();
        let category = category.to_string();
        let symbol = symbol.to_string();

        let message = BroadcastMessage {
            exchange: exchange,
            category: category,
            symbol: symbol,
            msg: message.clone(),
        };

        self.send_message(&message)
    }

    pub fn send_message(&self, message: &BroadcastMessage) -> anyhow::Result<usize> {
        let msg = serde_json::to_string(message).unwrap();
        let size = self.socket.send_to(msg.as_bytes(), &self.multicast_addr)?;

        Ok(size)
    }
}

const UDP_SIZE: usize = 4096;

#[derive(Debug)]
pub struct UdpReceiver {
    socket: Socket,
    buf: [MaybeUninit<u8>; UDP_SIZE],
}

impl UdpReceiver {
    // TODO: remove aget_id from param
    pub fn open() -> Self {
        let multicast_addr = Ipv4Addr::from_str(&env_rbot_multicast_addr());
        if multicast_addr.is_err() {
            log::error!("multicast_addr error {:?}", multicast_addr);
        }
        let multicast_addr = multicast_addr.unwrap();
        let multicast_port = env_rbot_multicast_port();

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();

        // Windowsの場合
        #[cfg(target_os = "windows")]
        {
            socket.set_reuse_address(true).unwrap();
            // socket.set_exclusive_address_use(false).unwrap();
        }

        // Windows以外の場合
        #[cfg(not(target_os = "windows"))]
        {
            socket.set_reuse_address(true).unwrap();
            // SO_REUSEPORTはLinuxとmacOSで利用可能ですが、Windowsでは利用できません。
            // そのため、このオプションを設定するコードはWindows以外でのみコンパイルされます。
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            socket.set_reuse_port(true).unwrap();
        }

        #[cfg(not(target_os = "windows"))]
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
            socket: socket,
            buf: buf,
        }
    }

    pub fn receive(&mut self) -> Result<String, std::io::Error> {
        let (amt, _addr) = self.socket.recv_from(&mut self.buf)?;

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

    pub async fn async_receive(&mut self) -> Result<String, std::io::Error> {
        let (amt, _addr) = self.socket.recv_from(&mut self.buf)?;

        let msg = &self.buf[..amt];
        let m = unsafe { std::mem::transmute::<_, &[u8]>(msg) };

        let msg = std::str::from_utf8(m).unwrap();
        Ok(msg.to_string())
    }

    pub async fn async_receive_message(&mut self) -> Result<BroadcastMessage, std::io::Error> {
        let msg = self.async_receive().await?;
        let msg = serde_json::from_str::<BroadcastMessage>(&msg)?;
        Ok(msg)
    }

    pub async fn receive_stream<'a>(
        &'a mut self,
        exchange: &'a str,
        category: &'a str,
        symbol: &'a str,
        agent_id: &'a str,
    ) -> impl Stream<Item = anyhow::Result<MarketMessage>> + 'a {
        stream! {
            loop {
                let msg = self.async_receive_message().await?;

                if msg.filter(exchange, category, symbol) {
                    match msg.msg {
                        MarketMessage::Order(ref order) => {
                            if order.is_my_order(agent_id) {
                                yield Ok(msg.msg);
                            }
                        }
                        _ => {
                            yield Ok(msg.msg);
                        }
                    }
                }
            }
        }
    }

    pub fn receive_market_message(&mut self) -> Result<MarketMessage, std::io::Error> {
        let msg = self.receive_message()?;

        let market_message: MarketMessage = msg.into();
        Ok(market_message)
    }

    pub fn open_channel(
        exchange: &str,
        category: &str,
        symbol: &str,
        agent_id: &str,
    ) -> anyhow::Result<Receiver<MarketMessage>> {
        let exchange = exchange.to_string();
        let category = category.to_string();
        let symbol = symbol.to_string();
        let agent_id = agent_id.to_string();

        let mut udp = Self::open();
        let (tx, rx) = crossbeam_channel::unbounded::<MarketMessage>();

        // TOD: change to async
        std::thread::spawn(move || loop {
            let msg = udp.receive_message();

            if msg.is_err() {
                break;
            }

            let msg = msg.unwrap();

            if msg.filter(&exchange, &category, &symbol) {
                let market_message = msg.msg.clone();

                match market_message {
                    MarketMessage::Order(ref order) => {
                        if order.is_my_order(&agent_id) {
                            let r = tx.send(market_message.clone());
                            if r.is_err() {
                                log::error!("open_channel: {}/{:?}", r.err().unwrap(), msg);
                                break;
                            }
                        }
                    }
                    _ => {
                        let r = tx.send(market_message.clone());
                        if r.is_err() {
                            log::error!("open_channel: {}/{:?}", r.err().unwrap(), msg);
                            break;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod test_udp {
    use crate::common::init_debug_log;
    use futures::stream::StreamExt;
    use tokio::task::spawn;

    #[test]
    fn send_test2() {
        let sender = super::UdpSender::open();
        let r = sender.send("hello world");

        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn receive_test3() {
        init_debug_log();

        // receive message
        let mut receiver = super::UdpReceiver::open();
        let mut count = 0;
        spawn({
            async move {
                loop {
                    let msg = receiver.receive();
                    assert!(msg.is_ok());
                    let msg = msg.unwrap();
                    println!("{}:{}", count, msg);

                    if 10 <= count {
                        break;
                    }
                    count += 1;
                }
            }
        });

        let sender = super::UdpSender::open();
        for i in 0..10 {
            let r = sender.send(&format!("hello world {}", i));
            assert!(r.is_ok());
        }
    }

    #[tokio::test]
    async fn test_open_channel() -> anyhow::Result<()> {
        init_debug_log();

        let receiver = super::UdpReceiver::open_channel("EXA", "linear", "BCTUSD", "AGENTID")?;

        spawn(async move {
            for i in 0..10 {
                let msg = receiver.recv().unwrap();
                println!("{}:{:?}", i, msg);
            }
        });

        let sender = super::UdpSender::open();
        for i in 0..10 {
            let r = sender.send(&format!("hello world {}", i));
            assert!(r.is_ok());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_receive_stream() -> anyhow::Result<()> {
        init_debug_log();

        let mut udp = super::UdpReceiver::open();

        spawn(async move {
            let receiver = udp.receive_stream("", "", "", "").await;
            let mut r = Box::pin(receiver);
            while let Some(msg) = r.next().await {
                let msg = msg.unwrap();
                println!("{:?}", msg);
            }
        });

        let sender = super::UdpSender::open();
        for i in 0..10 {
            let r = sender.send(&format!("hello world {}", i));
            assert!(r.is_ok());
        }

        Ok(())
    }
}
