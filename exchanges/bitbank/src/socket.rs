



#[cfg(test)]
mod tests {
    use rust_socketio::TransportType;

    use super::*;

    #[test]
    fn test_socket() {

        use rust_socketio::{ClientBuilder, Payload, RawClient};
        use serde_json::Value;
        use std::time::Duration;

        let callback = |payload: Payload, _socket: RawClient| {
            match payload {
                Payload::Binary(bytes) => println!("Received bytes: {:?}", bytes),
                Payload::Text(values) => println!("Received text: {:?}", values),
                _ => println!("Received unknown payload: {:?}", payload),
            }
        };

        let mut socket = ClientBuilder::new("wss://stream.bitbank.cc/socket.io/")
            .transport_type(TransportType::Websocket)
            .on("message", callback)
            .on("connect", callback)
            .on("disconnect", callback)
            .on("error", callback)
            .on("reconnect", callback)
            .on("reconnect_error", callback)
            .on("reconnect_failed", callback)
            .on("reconnect_attempt", callback)
            .connect()
            .expect("Failed to connect");


        // Subscribe to BTC/JPY trades
        socket
            .emit("join-room", "depth_diff_xrp_jpy")
            .expect("Failed to join room");

        // Keep connection alive for a few seconds to receive messages
        std::thread::sleep(Duration::from_secs(10));

        socket.disconnect().expect("Failed to disconnect");
    }
}

        