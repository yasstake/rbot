

#[cfg(test)]
mod test_all {
    use rbot_lib::common::ServerConfig;


    #[test]
    fn test_rest_get_board() {
        let config = bybit::BybitConfig::BTCUSDT();
        let server = bybit::BybitServerConfig::new(false);

        let board = rbot_server::get_rest_orderbook(&server.get_exchange_name(), &config);
        
        assert!(board.is_ok());

        println!("{:?}", board.unwrap());
    }

}