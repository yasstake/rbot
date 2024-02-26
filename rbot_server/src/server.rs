use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};

use actix_rt::System;
use rbot_lib::common::{get_orderbook_bin, get_orderbook_json, get_orderbook_list, MarketConfig, OrderBook, OrderBookList, ServerConfig};
use serde::{ser, Deserialize};
use log;

fn board_list() -> String {
    let list = get_orderbook_list();

    let mut result = String::new();

    for item in list {
        println!("{}", item);
        result.push_str(&format!("{}\n", item));
    }

    result
}


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

fn start_rest_server() -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || start_board_server());

    Ok(())
}

pub fn get_rest_orderbook(server: &dyn ServerConfig, config: &MarketConfig) -> anyhow::Result<OrderBook> {
    let path = OrderBookList::make_path(&server.get_exchange_name(), config);

    let r = reqwest::blocking::get(&format!("http://localhost:8080/board/vec/{}", path))?.bytes()?;

    let board = OrderBook::from_bin(server, config, r.to_vec())?;

    Ok(board)
}


#[cfg(test)]
mod test_rest_server {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_start_server_block() {
        let server = start_board_server();
        assert!(server.is_ok());
    }

    #[test]
    fn test_start_server() {
        let server = start_rest_server();
        assert!(server.is_ok());

        sleep(Duration::from_secs(120));
    }
}
