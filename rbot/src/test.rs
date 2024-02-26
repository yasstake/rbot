

#[cfg(test)]
mod test_all {

    #[test]
    fn test_rest_get_board() {
        let config = bybit::BybitConfig::BTCUSDT();
        let server = bybit::BybitServerConfig::new(false);


        let board = rbot_server::get_rest_orderbook(&server, &config);
        
        assert!(board.is_ok());

        println!("{:?}", board.unwrap());
    }

}