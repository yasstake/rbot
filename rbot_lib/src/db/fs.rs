// Copyright(c) 2022. yasstake. All rights reserved.

use std::fs;
use std::path::PathBuf;
use directories::ProjectDirs;
use once_cell::sync::Lazy;

use crate::common::env_rbot_db_root;


pub static DB_ROOT: Lazy<String> = Lazy::new(|| {
    if let Ok(path) = env_rbot_db_root() {
        return path;
    } else {
        "".to_string()
    }
});


pub fn project_dir() -> PathBuf {
    let proj_dir = ProjectDirs::from("net", "takibi", "rbot").unwrap();

    return proj_dir.data_dir().to_owned();
}

pub fn db_full_path(exchange_name: &str, category: &str, symbol: &str, base_dir: &str, test_net: bool) -> PathBuf {
    let path = DB_ROOT.to_string();    
    
    let project_dir = if base_dir != "" {
        PathBuf::from(base_dir)
    }
    else if path != "" {
        PathBuf::from(path)
    }
    else {
        project_dir()
    };

    let db_dir = project_dir.join("DB");
    let exchange_dir = db_dir.join(exchange_name);
    let _ = fs::create_dir_all(&exchange_dir);

    let mut db_name = format!("{}-{}.db", category, symbol);

    if test_net {
        db_name = format!("TEST-{}", db_name);
    }
    
    let db_path = exchange_dir.join(db_name);

    return db_path;
}


#[cfg(test)]
mod test_fs {
    use super::*;
    #[test]
    fn test_project_dir() {
        let path = project_dir();

        let db_name = path.join(".db");

        println!("{:?}", db_name);
    }

    #[test]
    fn test_db_full_path() {
        let db = db_full_path("FTX", "SPOT", "BTC-PERP", "/tmp", false);
        println!("{:?}", db);

        let db = db_full_path("FTX", "SPOT", "BTC-PERP", "/tmp/", true);
        println!("{:?}", db);

        let db = db_full_path("FTX", "SPOT", "BTC-PERP", "./", false);
        println!("{:?}", db);

    }
}