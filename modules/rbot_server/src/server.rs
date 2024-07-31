use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};

use rbot_lib::common::{get_orderbook_bin, get_orderbook_json, get_orderbook_list, MarketConfig, OrderBook, OrderBookList};
use serde_derive::Deserialize;
use log;

#[get("/")]
async fn greet() -> impl Responder {
    let board = get_orderbook_list();

    let mut result = String::new();

    result += "<html><head></head></body><ul>";
    for item in board.into_iter() {
        result += &format!(r#"<li><a href='./board/json/{}'>{}</a></li>"#, item, item);
    }
    result += "</ul>";

    let board = get_orderbook_list();    
    result += "<ul>";
    for item in board.into_iter() {
        result += &format!(r#"<li><a href='./board/vec/{}'>{}</a></li>"#, item, item);
    }
    result += "</ul></body></html>";

    HttpResponse::Ok().content_type(
        "text/html; charset=utf-8")
        .body(
        result)
}

#[derive(Deserialize)]
struct PathInfo {
    exchange: String,
    category: String,
    symbol: String,
}

#[get("/board/json/{exchange}/{category}/{symbol}")]
async fn get_board_json(path: web::Path<PathInfo>) -> impl Responder {
    let path = OrderBookList::make_path_from_str(&path.exchange, &path.category, &path.symbol);
    log::debug!("get_board_json: {:?}", path);

    let json = get_orderbook_json(&path, 0);

    if json.is_err() {
        return HttpResponse::NotFound().body(format!("Not Found {}", path));
    }

    HttpResponse::Ok().body(json.unwrap())
}

#[get("/board/vec/{exchange}/{category}/{symbol}")]
async fn get_board_vec(path: web::Path<PathInfo>) -> impl Responder {
    let path = OrderBookList::make_path_from_str(&path.exchange, &path.category, &path.symbol);
    log::debug!("get_board_json: {:?}", path);

    let vec = get_orderbook_bin(&path);

    if vec.is_err() {
        return HttpResponse::NotFound().body(format!("Not Found {}", path));
    }

    HttpResponse::Ok().body(vec.unwrap())
}



pub fn start_board_server() -> anyhow::Result<()> {
    let sys = actix_rt::System::new();

    let server = HttpServer::new(|| 
        App::new()
        .service(greet)
        .service(get_board_json)
        .service(get_board_vec)
        )
        
        .bind("127.0.0.1:8080")
        .expect("Failed to bind server")
        .run();

    sys.block_on(server).unwrap();

    Ok(())
}


pub fn get_rest_orderbook(config: &MarketConfig) -> anyhow::Result<OrderBook> {
    let path = OrderBookList::make_path(config);

    let r = reqwest::blocking::get(&format!("http://localhost:8080/board/vec/{}", path))?.bytes()?;

    let board = OrderBook::from_bin(config, r.to_vec())?;

    Ok(board)
}


#[cfg(test)]
mod test_rest_server {
    use super::*;

    #[test]
    fn test_start_server_block() {
        let server = start_board_server();
        assert!(server.is_ok());
    }
}
