
use std::sync::Mutex;
use once_cell::sync::Lazy;

const BUFFER_SIZE: usize = 4096;

static MESSAGE_BUFFER: Lazy<Mutex<Vec<String>>> =
Lazy::new(|| Mutex::new(Vec::<String>::new()));

pub fn write_agent_messsage(message: &str) {
    let mut lock = MESSAGE_BUFFER.lock().unwrap();

    println!("set: {}", message);

    lock.push(message.to_string());
    if BUFFER_SIZE < lock.len(){
        lock.pop();
    }
}

pub fn get_agent_message() -> Vec<String> {
    let mut lock = MESSAGE_BUFFER.lock().unwrap();
   
    let message = lock.clone();
    lock.clear();    

    if lock.len() != 0 {    
        println!("GET!! {:?}", message);
    }

    message
}


#[cfg(test)]
mod test_message_write{
    use super::{get_agent_message, write_agent_messsage};

    #[test]
    fn test_write_and_read() {
        write_agent_messsage("test1");
        write_agent_messsage("test2");

        let msg = get_agent_message();
        println!("{:?}", msg);
    }


}