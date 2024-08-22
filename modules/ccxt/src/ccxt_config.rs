

fn read_json() {
    let json_str = include_str!("./exchange.json");    

    let json_value: Result<serde_json::Value, serde_json::Error> = serde_json::from_str(json_str);
    match json_value {
        Ok(value) => println!("JSON value: {:?}", value),
        Err(e) => eprintln!("Error parsing JSON: {}", e),
    }
}

#[test]
fn test_read_json() {
    read_json()
}