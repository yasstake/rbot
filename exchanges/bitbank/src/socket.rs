use pubnub::PubNubClientBuilder;
use tracing::subscriber::NoSubscriber;

const SUBSCRIBER_KEY: &str = "sub-c-ecebae8e-dd60-11e6-b6b1-02ee2ddab7fe";

#[cfg(test)]
mod tests {
    use pubnub::Keyset;
    use tracing::instrument::WithSubscriber;

    use super::*;

    /*
    #[tokio::test]
    async fn test_pubnub() -> anyhow::Result<()> {
        let client = PubNubClientBuilder::with_reqwest_transport()
        .with_keyset(Keyset {
             publish_key: Some("pub-c-abc123"),
             subscribe_key: "sub-c-abc123",
             secret_key: None,
        })
        .with_user_id("my-user-id")
        .build()?;

        let channel = client.channel("depth_diff_xrp_jpy");
        let subscription = channel.subscribe_with_timetoken(None).await?;

        for _i in 0..10 {
            let message = subscription.next().await?;
            println!("Received message: {:?}", message);
        }

        // Listen for messages

        Ok(())
    }
    */
}


#[cfg(test)]
mod test2 {
    use pubnub::subscribe::Subscriber;
    use futures::StreamExt;
    use tokio::time::sleep;
    use std::time::Duration;
    use serde_json;
    use pubnub::{
        dx::subscribe::Update,
        subscribe::EventSubscriber,
        Keyset, PubNubClientBuilder,
    };
    #[tokio::test]
    async fn test_main() -> Result<(), Box<dyn std::error::Error>> {
        use pubnub::subscribe::{EventEmitter, SubscriptionParams};
        let publish_key = "my_publish_key";
        let subscribe_key = "my_subscribe_key";


        let client = PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key,
                publish_key: Some(publish_key),
                secret_key: None,
            })
            .with_user_id("user_id")
            .build()?;
    
        println!("PubNub instance created");
    
        let subscription = client.subscription(SubscriptionParams {
            channels: Some(&["my_channel"]),
            channel_groups: None,
            options: None
        });
    
        let channel_entity = client.channel("my_channel_2");
        let channel_entity_subscription = channel_entity.subscription(None);
    
        subscription.subscribe();
        channel_entity_subscription.subscribe();
    
        println!("Subscribed to channels");
    
        // Launch a new task to print out each received message
        tokio::spawn(client.status_stream().for_each(|status| async move {
            println!("\nStatus: {:?}", status)
        }));
        tokio::spawn(subscription.stream().for_each(|event| async move {
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
        }));
    
        // Explicitly listen only for real-time `message` updates.
        tokio::spawn(
            channel_entity_subscription
                .messages_stream()
                .for_each(|message| async move {
                    if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
                        if let Ok(cleaned) = serde_json::from_str::<String>(&utf8_message) {
                            println!("message: {}", cleaned);
                        }
                    }
                }),
        );
    
       sleep(Duration::from_secs(2)).await;
    
        // Send a message to the channel
        client
            .publish_message("hello world!")
            .channel("my_channel")
            .r#type("text-message")
            .execute()
            .await?;
    
       // Send a message to another channel
        client
            .publish_message("hello world on the other channel!")
            .channel("my_channel_2")
            .r#type("text-message")
            .execute()
            .await?;
    
        sleep(Duration::from_secs(15)).await;
    
        Ok(())
    }
}