// Copyright(c) 2022. yasstake. All rights reserved.

pub mod binance;

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
        // TODO: implmeent extract gzip file.
//        return gzip_log_download(response, has_header, f);
        return Ok(0);
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
