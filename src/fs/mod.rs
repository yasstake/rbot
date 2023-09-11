// Copyright(c) 2022. yasstake. All rights reserved.

use std::fs;
use std::path::{PathBuf};
use directories::ProjectDirs;


pub fn project_dir() -> PathBuf {
    let proj_dir = ProjectDirs::from("net", "takibi", "rbot").unwrap();

    return proj_dir.data_dir().to_owned();
}


pub fn db_full_path(exchange_name: &str, category: &str, symbol: &str) -> PathBuf {
    let project_dir = project_dir();
    let db_dir = project_dir.join("DB");
    let exchange_dir = db_dir.join(exchange_name);
    let _ = fs::create_dir_all(&exchange_dir);

    let db_name = format!("{}-{}.db", category, symbol);
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
        let db = db_full_path("FTX", "SPOT", "BTC-PERP");

        println!("{:?}", db);
    }
}