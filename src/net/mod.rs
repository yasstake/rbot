
pub mod udp;
pub mod rest;

pub use udp::*;
pub use rest::*;

/// RestServer
/// 
pub struct RestServer {
    
}

impl RestServer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn start(&self) {
        println!("RestServer start");
    }
}



    