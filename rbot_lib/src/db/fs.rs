// Copyright(c) 2022. yasstake. All rights reserved.

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use directories::ProjectDirs;
use once_cell::sync::Lazy;
use pyo3::pyfunction;

use crate::common::{date_string, env_rbot_db_root, MicroSec, FLOOR_DAY};


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

pub fn db_path_root(exchange_name: &str, category: &str, symbol: &str, production: bool) -> PathBuf {
    let project_dir = get_db_root();
    let project_dir = PathBuf::from(project_dir);
    
    let db_dir = project_dir.join("DB");
    let exchange_dir = db_dir.join(exchange_name);
    let category_dir = exchange_dir.join(category);
    let symbol_dir = category_dir.join(symbol);

    let db_root = if production {
        symbol_dir.join("PRODUCTION")        
    } else {
        symbol_dir.join("TEST")
    };

    let _ = fs::create_dir_all(&db_root);

    return db_root;
}

pub fn archive_directory(exchange_name: &str, category: &str, symbol: &str, production: bool) -> PathBuf {
    let db_path_root = db_path_root(exchange_name, category, symbol, production);

    let archive_dir = db_path_root.join("ARCHIVE");
    let _ = fs::create_dir_all(&archive_dir);    

    return archive_dir;
}

pub fn archive_full_path(exchange_name: &str, category: &str, symbol: &str, production: bool, date: MicroSec) -> PathBuf {
    let archive_dir = archive_directory(exchange_name, category, symbol, production);

    let date = FLOOR_DAY(date);
    let date = date_string(date);

    let archive_name = format!("{}-{}-{}.csv.gz", date, category, symbol);
    let archive_path = archive_dir.join(archive_name);

    return archive_path;
}

pub fn has_archive_file(exchange_name: &str, category: &str, symbol: &str, production: bool, date: MicroSec) -> bool {
    let archive_path = archive_full_path(exchange_name, category, symbol, production, date);

    return archive_path.exists();
}

pub fn db_full_path(exchange_name: &str, category: &str, symbol: &str, production: bool) -> PathBuf {
    let db_path_root = db_path_root(exchange_name, category, symbol, production);

    let db_name = format!("{}-{}.db", category, symbol);
    
    let db_path = db_path_root.join(db_name);

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

        let archive = archive_directory("FTX", "SPOT", "BTC-PERP", false);
        print!("{:?}", archive);
    }
}