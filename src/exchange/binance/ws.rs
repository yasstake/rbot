use tokio::net::TcpStream;
use url::Url;

//const WSS_URL: &str = "wss://ws-api.binance.com:443/ws-api/v3";
// const WSS_URL: &str = "wss://testnet.binance.vision/ws-api/v3";
const WSS_URL: &str = "wss://stream.binance.com:443/ws/btcusdt@trade";

/*
const SUBSCRIBE_MESSAGE: &str = r#"{"id": "409a20bd-253d-41db-a6dd-687862a5882f",
                                    "method": "trades.trade",
                                    "params": {"symbol": "BNBBTC", "limit": 10}
                                }"#;

                                {
*/
const SUBSCRIBE_MESSAGE: &str = r#"{"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}"#;

use tungstenite::error::Error as TungsError;
use tungstenite::protocol::Message;

use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::{self, MaybeTlsStream, WebSocketStream};
use tungstenite;

/*
async fn connect_to_wss(wss_url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, Error> {
    let url = Url::parse(wss_url)?;

    let result = connect_async(url).await;

    Ok(stream)
}
*/

#[cfg(test)]
mod test_websocket {
    use core::panic;
    use std::time::Duration;
    use url::Url;

    use tungstenite;
    use tungstenite::protocol::Message;
    use tokio_tungstenite::connect_async;

    use futures_util::sink::SinkExt;
    use futures_util::stream::StreamExt;

    const SUBSCRIBE_MESSAGE: &str = r#"{"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}"#;
    const WSS_URL: &str = "wss://stream.binance.com:443/ws/btcusdt@trade";

    #[tokio::test]
    async fn test_connect() {
        let wss_url = WSS_URL;
        let url = Url::parse(wss_url).unwrap();
        let result = connect_async(url).await;

        let (ws_stream, _ws_response) = match result {
            Ok(connection) => connection,
            Err(e) => {
                eprintln!("Connect Error {:?}", e);
                panic!();
            }
        };

        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(60));

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        match ws_sender.send(SUBSCRIBE_MESSAGE.into()).await {
            Ok(_) => println!("subscribed message {}", SUBSCRIBE_MESSAGE),
            Err(e) => println!("Error {:?}", e),
        };

        'receive_loop: loop {
            tokio::select! {
                msg_item = ws_receiver.next() => {
                    match msg_item {
                        Some(msg) => {
                            match msg {
                                Ok(Message::Text(json_string)) => {
                                    println!("{}", json_string);
                                },
                                Ok(Message::Ping(message)) => println!("# Ping msg={:#?}", message),
                                Ok(Message::Pong(message)) => println!("# Pong msg={:#?}", message),
                                Ok(Message::Binary(binary)) => println!("> BINMESSAGE / {:?}", binary),
                                Ok(Message::Frame(frame)) => println!("> FRAME      / {:?}", frame),
                                Ok(Message::Close(close_option)) => {
                                       println!("CLOSED by PEER {:?}", close_option);
                                       break 'receive_loop;
                                },
                                Err(e) => {
                                    eprintln!("{:?}", e);
                                },
                            }
                        },
                        None => (),
                    }
                }

                // Hart beat.
                _ = heartbeat_interval.tick() => {
                    match ws_sender.send(Message::Ping(vec![])).await {
                        Ok(_) => println!("send ping message"),
                        Err(e) => eprintln!("error sending ping message; err={}", e),
                    }
                }

            }
        }
    }
}
