// Copyright(c) 2022. yasstake. All rights reserved.

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use directories::ProjectDirs;
use once_cell::sync::Lazy;
use pyo3::pyfunction;

use crate::common::env_rbot_db_root;


#[pyfunction]
pub fn get_db_root() -> String {
    DB_ROOT.lock().unwrap().to_string()
}

#[pyfunction]
pub fn set_db_root(path: &str) {
    let mut root_path = DB_ROOT.lock().unwrap();

    *root_path = path.to_string();
}

pub static DB_ROOT: Lazy<Mutex<String>> = Lazy::new(|| Mutex::new({
    if let Ok(path) = env_rbot_db_root() {
        path
    } else {
        project_dir()        
    }
}));


pub fn project_dir() -> String {
    let proj_dir = ProjectDirs::from("net", "takibi", "rbot").unwrap();

    return proj_dir.data_dir().to_str().unwrap().to_string();
}

pub fn db_full_path(exchange_name: &str, category: &str, symbol: &str, test_net: bool) -> PathBuf {
    let project_dir = get_db_root();
    let project_dir = PathBuf::from(project_dir);
    
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
        let path = PathBuf::from(path);

        let db_name = path.join(".db");

        println!("{:?}", db_name);
    }

    #[test]
    fn test_db_full_path() {
        let db = db_full_path("FTX", "SPOT", "BTC-PERP",  false);
        println!("{:?}", db);

        let db = db_full_path("FTX", "SPOT", "BTC-PERP",  true);
        println!("{:?}", db);

        let db = db_full_path("FTX", "SPOT", "BTC-PERP", false);
        println!("{:?}", db);

    }
}