// Copyright(c) 2023. yasstake. All rights reserved.
// Abloultely no warranty.

pub mod binance;
pub mod bb;
// pub mod ftx;

use std::{
    fs::File,
    io::{copy, Write, BufReader, Cursor},
    path::Path,
};

use csv::{self, StringRecord};
use flate2::bufread::GzDecoder;
use tempfile::tempdir;
use tokio::runtime::Runtime;
use zip::ZipArchive;

use crate::common::order::Trade;

use std::sync::mpsc::Sender;

pub async fn log_download_tmp(url: &str, tmp_dir: &Path) -> Result<String, String> {
    let response = match reqwest::get(url).await {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    let fname = response
        .url()
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .unwrap_or("tmp.bin");

    let fname = tmp_dir.join(fname);

    let mut target = match File::create(&fname) {
        Ok(t) => t,
        Err(e) => {return Err(e.to_string());}
    };

    let file_name = fname.to_str().unwrap();
    let content = match response.bytes().await {
        Ok(c) => c,
        Err(e) => {
            log::error!("{}", e.to_string());
            return Err(e.to_string());
        }
    };
    let mut cursor = Cursor::new(content);

    if copy(&mut cursor, &mut target).is_err() {
       return Err(format!("write error"));
    }

    let _r = target.flush();

    log::debug!("download size {}", target.metadata().unwrap().len());

    Ok(file_name.to_string())
}

pub fn log_download<F>(url: &str, has_header: bool, f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("Downloading ...[{}]", url);
    println!("Downloading ...[{}]", url);    

    let rt = match Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            log::error!("runtie create error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    let tmp_dir = match tempdir() {
        Ok(tmp) => tmp,
        Err(e) => {
            log::error!("create tmp dir error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    let result = rt.block_on(async { log_download_tmp(url, tmp_dir.path()).await });

    let file_path: String;
    match result {
        Ok(path) => {
            file_path = path;
        }
        Err(e) => {
            return Err(e);
        }
    }

    log::debug!("let's extract = {}", file_path);
   
    if url.ends_with("gz") || url.ends_with("GZ") {
        return extract_gzip_log(&file_path, has_header, f);
    } else if url.ends_with("zip") || url.ends_with("ZIP") {
        return extract_zip_log(&file_path, has_header, f);    
    } else {
        log::error!("unknown file suffix {}", url);
        return Err(format!("").to_string());
    }

    // remove tmp file
}


#[allow(unused)]
fn gzip_log_download<F>(
    response: reqwest::blocking::Response,
    has_header: bool,
    mut f: F,
) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    let mut rec_count = 0;

    match response.bytes() {
        Ok(b) => {
            let gz = GzDecoder::new(b.as_ref());

            let mut reader = csv::Reader::from_reader(gz);
            if has_header {
                reader.has_headers();
            }

            for rec in reader.records() {
                if let Ok(string_rec) = rec {
                    f(&string_rec);
                    rec_count += 1;
                }
            }
        }
        Err(e) => {
            log::error!("{}", e);
            return Err(e.to_string());
        }
    }
    Ok(rec_count)
}

#[allow(unused)]
fn zip_log_download<F>(
    response: reqwest::blocking::Response,
    has_header: bool,
    mut f: F,
) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    let mut rec_count = 0;

    match response.bytes() {
        Ok(b) => {
            let reader = std::io::Cursor::new(b);
            let mut zip = zip::ZipArchive::new(reader).unwrap();

            for i in 0..zip.len() {
                let mut file = zip.by_index(i).unwrap();

                if file.name().ends_with("csv") == false {
                    log::debug!("Skip file {}", file.name());
                    continue;
                }

                let mut csv_reader = csv::Reader::from_reader(file);
                if has_header {
                    csv_reader.has_headers();
                }
                for rec in csv_reader.records() {
                    if let Ok(string_rec) = rec {
                        f(&string_rec);
                        rec_count += 1;
                    }
                }
            }
        }
        Err(e) => {
            log::error!("{}", e);
            return Err(e.to_string());
        }
    }
    Ok(rec_count)
}

fn extract_zip_log<F>(path: &String, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("extract zip = {}", path);
    let mut rec_count = 0;

    let file_path = Path::new(path);

    if file_path.exists() == false {
        log::error!("File Not Found {}", path);
        return Err(format!("File Not Found {}", path));
    }

    let tmp_file = File::open(file_path).unwrap();
    let bufreader = BufReader::new(tmp_file);

    let mut zip = match ZipArchive::new(bufreader) {
        Ok(z) => {
            z
        }
        Err(e) => {
            return Err(e.to_string());
        }
    };

    for i in 0..zip.len() {
        let file = zip.by_index(i).unwrap();

        if file.name().to_lowercase().ends_with("csv") == false {
            log::debug!("Skip file {}", file.name());
            continue;
        }
        else {
            log::debug!("processing {}", file.name());
        }

        let mut csv_reader = csv::Reader::from_reader(file);
        if has_header {
            csv_reader.has_headers();
        }
        for rec in csv_reader.records() {
            if let Ok(string_rec) = rec {
                f(&string_rec);
                rec_count += 1;
            }
        }
    }

    Ok(rec_count)
}


fn extract_gzip_log<F>(path: &String, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("extract gzip = {}", path);
    let mut rec_count = 0;

    let file_path = Path::new(path);

    if file_path.exists() == false {
        log::error!("File Not Found {}", path);
        return Err(format!("File Not Found {}", path));
    }

    let tmp_file = File::open(file_path).unwrap();
    let bufreader = BufReader::new(tmp_file);
    let gzip_reader = std::io::BufReader::new(GzDecoder::new(bufreader));

    let mut csv_reader = csv::Reader::from_reader(gzip_reader);
    if has_header {
        csv_reader.has_headers();
    }
    for rec in csv_reader.records() {
        if let Ok(string_rec) = rec {
            f(&string_rec);
            rec_count += 1;
        }
    }

    Ok(rec_count)
}

fn make_download_url_list<F>(name: &str, days: Vec<i64>, f: F) -> Vec<String> 
    where F: Fn(&str, i64) -> String
{
    let mut urls: Vec<String> = vec![];
    for day in days {
        urls.push(f(name, day));
    }
    urls
}



fn download_log<F>(urls: Vec<String>, tx: Sender<Vec<Trade>>, f: F) -> i64
    where F: Fn(&StringRecord) -> Trade
     {
    let mut download_rec = 0;

    for url in urls {
        log::debug!("download url = {}", url);

        let mut buffer: Vec<Trade> = vec![];

        let result = log_download(url.as_str(), false, |rec| {
            let trade = f(&rec);

            buffer.push(trade);

            if 2000 < buffer.len() {
                let result = tx.send(buffer.to_vec());

                match result {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{:?}", e);
                    }
                }
                buffer.clear();
            }
        });

        if buffer.len() != 0 {
            let result = tx.send(buffer.to_vec());
            match result {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{:?}", e);
                }
            }

            buffer.clear();
        }

        match result {
            Ok(count) => {
                log::debug!("Downloaded rec = {} ", count);
                println!("Downloaded rec = {} ", count);
                download_rec += count;
            }
            Err(e) => {
                log::error!("extract err = {}", e.as_str());
            }
        }
    }

    log::debug!("download rec = {}", download_rec);

    return download_rec;

}



#[cfg(test)]
mod test_exchange {
    use super::*;
    use crate::common::init_debug_log;

    #[test]
    fn test_log_download() {
        init_debug_log();
    }

    #[tokio::test]
    async fn log_download_temp_test() {
        init_debug_log();
        let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";
        let tmp_dir = tempdir().unwrap();
        let _r = log_download_tmp(url, tmp_dir.path()).await;
    }

}
