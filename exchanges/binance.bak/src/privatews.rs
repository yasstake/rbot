
pub struct BinancePrivateWsClient {
    ws: AutoConnectClient<BinanceServerConfig, BinanceWsOpMessage>,
    handler: Option<JoinHandle<()>>,
    listen_key: String,
}


impl BinancePrivateWsClient {
    pub async fn new(server: &BinanceServerConfig) -> Self {
        let dummy_config = MarketConfig::new("dummy", "dummy", "dummy", 0, 0, 0); 

        let mut private_ws = AutoConnectClient::new(
            server,
            &dummy_config,
            &server.get_user_ws_server(),
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS,
            None,
            None,
        );

        private_ws.subscribe(&server..config.private_subscribe_channel).await;

        Self {
            ws: private_ws,
            handler: None,
        }
    }


    pub async fn connect(&mut self) {
        self.ws.connect().await
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a {
        let mut s = Box::pin(self.ws.open_stream().await);

        stream! {
            while let Some(message) = s.next().await {
                match message {
                    Ok(m) => {
                        match Self::parse_message(m) {
                            Err(e) => {
                                println!("Parse Error: {:?}", e);
                                continue;
                            }
                            Ok(m) => {
                                let market_message = Self::convert_ws_message(m);

                                match market_message
                                {
                                    Err(e) => {
                                        println!("Convert Error: {:?}", e);
                                        continue;
                                    }
                                    Ok(m) => {
                                        yield Ok(m);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Receive Error: {:?}", e);
                    }
                }
            }
        }
    }

    fn parse_message(message: String) -> anyhow::Result<BinanceUserWsMessage> {
        let m = serde_json::from_str::<BinanceUserWsMessage>(&message);

        if m.is_err() {
            log::warn!("Error in serde_json::from_str: {:?}", message);
            return Err(anyhow!("Error in serde_json::from_str: {:?}", message));
        }

        Ok(m.unwrap())
    }

    fn convert_ws_message(message: BinanceUserWsMessage) -> anyhow::Result<MultiMarketMessage> {
        //Ok(message.into())
        todo!();
    }
}
