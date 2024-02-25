use actix_web::{web, App, HttpResponse, HttpServer, Responder, get};

use rbot_lib::common::{get_order_book_json, OrderBookList};
use serde::{ser, Deserialize};
use actix_rt::System;

#[get("/")]
async fn greet() -> impl Responder {
    HttpResponse::Ok().body("Hello, World!")
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

    let json = get_order_book_json(&path, 0);
    
    if json.is_err() {
        return HttpResponse::NotFound().body(format!("Not Found {}", path));
    }

    HttpResponse::Ok().body(json.unwrap())
}

fn start_server() -> anyhow::Result<()> {
    let sys = actix_rt::System::new();

    let server = HttpServer::new(|| {
        App::new()
        .service(greet)
    })
    .bind("127.0.0.1:8080").expect("Failed to bind server")
    .run();

    sys.block_on(server).unwrap();

    Ok(())
}

fn start_rest_server() -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        start_server()
    });

    Ok(())
}


#[cfg(test)]
mod test_rest_server {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;
    

    #[test]
    fn test_start_server_block(){
        let server = start_server();
        assert!(server.is_ok());
    }


    #[test]
    fn test_start_server() {
        let server = start_rest_server();
        assert!(server.is_ok());


        sleep(Duration::from_secs(120));
    }
}