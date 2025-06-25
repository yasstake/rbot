use futures::StreamExt;
use log::debug;
use pubnub::subscribe::Subscriber;
use pubnub::{dx::subscribe::Update, subscribe::EventSubscriber, Keyset, PubNubClientBuilder};
use rbot_lib::common::{init_debug_log, ExchangeConfig};
use serde_json;
use std::time::Duration;
use tokio::time::sleep;

use crate::BitbankRestApi;

const PUBNUB_SUB_KEY: &str = "sub-c-ecebae8e-dd60-11e6-b6b1-02ee2ddab7fe";

#[tokio::test]
async fn test_main() -> Result<(), Box<dyn std::error::Error>> {
    init_debug_log();

    let server = ExchangeConfig::open("bitbank", true)?;
    let keys = BitbankRestApi::new(&server)
        .get_private_stream_key()
        .await?;

    debug!("channel: {:?}", keys.pubnub_channel);
    debug!("token: {:?}", keys.pubnub_token);

    use pubnub::subscribe::{EventEmitter, SubscriptionParams};
    let subscribe_key = PUBNUB_SUB_KEY;

    let pubnub = PubNubClientBuilder::with_reqwest_transport()
        .with_keyset(Keyset {
            subscribe_key,
            publish_key: None,
            secret_key: None,
        })
        .with_user_id(&keys.pubnub_channel)
        .build()?;

    pubnub.set_token(keys.pubnub_token);

    let subscription = pubnub.subscription(
        SubscriptionParams{
            channels: Some(&[&keys.pubnub_channel]),
            channel_groups: None,
            options: None,
        }
    );

    subscription.subscribe();

    // Launch a new task to handle status events
    tokio::spawn(
        pubnub
            .status_stream()
            .for_each(|status| async move { 
                println!("\nStatus: {:?}", status) 
            }),
    );

    tokio::spawn(
        subscription
            .stream()
            .for_each(|message| async move {
                 debug!("\nmessage: {:?}", message);

                 match message {
                    Update::Message(message) => {
                        if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
                            debug!("\nmessage: {:?}", utf8_message);
                        }
                    },
                    Update::Signal(message) => {
                        debug!("\nsignal: {:?}", message);
                    }
                    _ => {
                        debug!("\nother: {:?}", message);
                    }
                 }

            }),
    );

    // Keep the test running for a bit to receive messages
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    Ok(())
}

#[tokio::test]
async fn test_main2() -> Result<(), Box<dyn std::error::Error>> {
    init_debug_log();

    let server = ExchangeConfig::open("bitbank", true)?;
    let keys = BitbankRestApi::new(&server)
        .get_private_stream_key()
        .await?;

    println!("keys: {:?}", keys);

    use pubnub::subscribe::{EventEmitter, SubscriptionParams};
    let subscribe_key = PUBNUB_SUB_KEY;

    let client = PubNubClientBuilder::with_reqwest_transport()
        .with_keyset(Keyset {
            subscribe_key,
            publish_key: None,
            secret_key: None,
        })
        .with_user_id(&keys.pubnub_channel)
        .build()?;

    tokio::spawn(
        client
            .status_stream()
            .for_each(|status| async move { println!("\nStatus: {:?}", status) }),
    );

    println!("PubNub instance created");

    client.set_token(keys.pubnub_token);

    let channel_entity = client.channel(&keys.pubnub_channel);
    let channel_entity_subscription = channel_entity.subscription(None);

    println!("Subscribed to channels");

    // Launch a new task to print out each received message

    tokio::spawn(
        channel_entity_subscription
            .stream()
            .for_each(|event| async move {
                match event {
                    Update::Message(message) | Update::Signal(message) => {
                        // Silently log if UTF-8 conversion fails
                        if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
                            if let Ok(cleaned) = serde_json::from_str::<String>(&utf8_message) {
                                println!("message: {}", cleaned);
                            }
                        }
                    }
                    Update::Presence(presence) => {
                        println!("presence: {:?}", presence)
                    }
                    Update::AppContext(object) => {
                        println!("object: {:?}", object)
                    }
                    Update::MessageAction(action) => {
                        println!("message action: {:?}", action)
                    }
                    Update::File(file) => {
                        println!("file: {:?}", file)
                    }
                }
            }),
    );

    // Explicitly listen only for real-time `message` updates.

    sleep(Duration::from_secs(30)).await;

    Ok(())
}

#[tokio::test]
async fn test_main3() -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt;

    let server = ExchangeConfig::open("bitbank", true)?;
    let keys = BitbankRestApi::new(&server)
        .get_private_stream_key()
        .await?;

    println!("keys: {:?}", keys);

    use pubnub::subscribe::{EventEmitter, SubscriptionParams};
    let subscribe_key = PUBNUB_SUB_KEY;

    let client = PubNubClientBuilder::with_reqwest_transport()
        .with_keyset(Keyset {
            subscribe_key,
            publish_key: None,
            secret_key: None,
        })
        .with_user_id(&keys.pubnub_channel)
        .build()?;

    client.set_token(keys.pubnub_token);

    let subscription = client.subscription(
        SubscriptionParams{
            channels: Some(&[&keys.pubnub_channel]),
            channel_groups: None,
            options: None,
        }
    );

    subscription.subscribe();

    tokio::spawn(
        subscription
            .stream()
            .for_each(|message| async move {
                match message {
                    Update::Message(message) => {
                        if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
                            println!("message: {}", utf8_message);
                        }
                    },
                    Update::Signal(message) => {
                        println!("signal: {:?}", message);
                    }
                    _ => {
                        println!("other: {:?}", message);
                    }
                }
            }),
    );

    sleep(Duration::from_secs(30)).await;

    Ok(())
}

