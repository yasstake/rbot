use std::error::Error;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message};

use tokio::runtime::Runtime;

use url::Url;

const ENDPOINT: &str = "wss://stream.bybit.com/realtime";


