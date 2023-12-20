// Copyright(c) 2023. yasstake. All rights reserved.
// Abloultely no warranty.

use std::{
    fs::File,
    io::{copy, BufReader, Cursor, Write},
    path::Path, thread::sleep, time::Duration,
};

use crate::common::{LogStatus, Trade, flush_log};
use crossbeam_channel::Sender;
use csv::{self, StringRecord};
use flate2::bufread::GzDecoder;
use reqwest::Method;
use tempfile::tempdir;
use zip::ZipArchive;

pub fn log_download_tmp(url: &str, tmp_dir: &Path) -> Result<String, String> {
    let client = reqwest::blocking::Client::new();

    let response = match client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(format!("URL get error{}", e.to_string()));
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap_or_default() // if error, return 0
    );

    if ! response.status().is_success() {
        return Err(format!("Err: response code {}", response.status().as_str()));
    }

    let fname = response
        .url()
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .unwrap_or("tmp.bin");

    let fname = tmp_dir.join(fname);

    let mut target = match File::create(&fname) {
        Ok(t) => t,
        Err(e) => {
            return Err(format!("file create error {}", e.to_string()));
        }
    };

    let file_name = fname.to_str().unwrap();
    let content = match response.bytes() {
        Ok(c) => c,
        Err(e) => {
            log::error!("{}", e.to_string());
            return Err(format!("log_download_tmp err{}", e.to_string()));
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

    let tmp_dir = match tempdir() {
        Ok(tmp) => tmp,
        Err(e) => {
            log::error!("create tmp dir error {}", e.to_string());
            return Err(format!("create tmp dir error {}", e.to_string()));
        }
    };

    let result = log_download_tmp(url, tmp_dir.path());

    let file_path = match result {
        Ok(path) => {
            path
        }
        Err(e) => {
            log::error!("download error {}", e.to_string());
            return Err(format!("download error{}", e));
        }
    };

    log::debug!("let's extract = {}", file_path);

    if url.ends_with("gz") || url.ends_with("GZ") {
        log::debug!("extract gzip = {}", file_path);
        return extract_gzip_log(&file_path, has_header, f);
    } else if url.ends_with("zip") || url.ends_with("ZIP") {
        log::debug!("extract zip = {}", file_path);
        return extract_zip_log(&file_path, has_header, f);
    } else {
        log::error!("unknown file suffix {}", url);
        return Err(format!("unknown file suffix").to_string());
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
            return Err(format!("gzip_log_download_error {}", e.to_string()));
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
            return Err(format!("zip_log_download error {}", e.to_string()));
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
        Ok(z) => z,
        Err(e) => {
            return Err(format!("extract zip log error {}",e.to_string()));
        }
    };

    for i in 0..zip.len() {
        let file = zip.by_index(i).unwrap();

        if file.name().to_lowercase().ends_with("csv") == false {
            log::debug!("Skip file {}", file.name());
            continue;
        } else {
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

pub fn extract_gzip_log<F>(path: &String, has_header: bool, mut f: F) -> Result<i64, String>
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

pub fn make_download_url_list<F>(name: &str, days: Vec<i64>, f: F) -> Vec<String>
where
    F: Fn(&str, i64) -> String,
{
    let mut urls: Vec<String> = vec![];
    for day in days {
        urls.push(f(name, day));
    }
    urls
}

pub fn download_logs<F>(
    urls: Vec<String>,
    tx: Sender<Vec<Trade>>,
    has_header: bool,
    verbose: bool,
    f: F,
) -> Result<i64, String>
where
    F: Fn(&StringRecord) -> Trade,
{
    let mut download_rec = 0;

    for url in urls {
        match download_log(&url, &tx, has_header, verbose, &f) {
            Ok(count) => {
                download_rec += count;
            }
            Err(e) => {
                log::error!("download error {}", e);
                if verbose {
                    println!("skip log url={} / {}", url, e);
                }
            }
        }
    }

    return Ok(download_rec);
}

const MAX_BUFFER_SIZE: usize = 2000;
const MAX_QUEUE_SIZE: usize = 50;

pub fn download_log<F>(
    url: &String,
    tx: &Sender<Vec<Trade>>,
    has_header: bool,
    verbose: bool,
    f: &F,
) -> Result<i64, String>
where
    F: Fn(&StringRecord) -> Trade,
{
    // TODO:  レコードが割り切れる場合、最後のレコードのstatusをFixBlockEndにする。
    if verbose {
        print!("log download (url = {})", url);
        flush_log();
    }
    let mut download_rec = 0;

    let mut buffer: Vec<Trade> = vec![];
    let mut is_first_record = true;

    let result = log_download(url.as_str(), has_header, |rec| {
        let mut trade = f(&rec);
        trade.status = LogStatus::FixArchiveBlock;

        buffer.push(trade);

        if MAX_BUFFER_SIZE < buffer.len() {
            if is_first_record {
                buffer[0].status = LogStatus::FixBlockStart;
                is_first_record = false;
            }

            while MAX_QUEUE_SIZE < tx.len() {
                sleep(Duration::from_millis(100));
            }

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

    let buffer_len = buffer.len();

    if buffer_len != 0 {
        buffer[buffer_len - 1].status = LogStatus::FixBlockEnd;

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
            download_rec += count;
        }
        Err(e) => {
            log::error!("extract err = {}", e.as_str());
            return Err(format!("extract err = {}", e.as_str()));
        }
    }

    log::debug!("download rec = {}", download_rec);
    if verbose {
        println!(" download complete rec = {}", download_rec);
        flush_log();
    }

    return Ok(download_rec);
}

pub fn do_rest_request(
    method: Method,
    url: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let client = reqwest::blocking::Client::new();

    let mut request_builder = client.request(method.clone(), url);

    // make request builder as a common function.
    for (key, value) in headers {
        request_builder = request_builder.header(key, value);
    }

    if body != "" {
        request_builder = request_builder.body(body.to_string());
    }

    request_builder = request_builder
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html");

    let response = match request_builder.send() {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(format!("URL get error {}, ", e.to_string()));
        }
    };

    log::debug!(
        "Response code = {} / download size {:?} / method({:?}) / URL = {} / path{}",
        response.status().as_str(),
        response.content_length(),
        method,
        url,
        body
    );

    Ok(response.text().unwrap())
}

pub fn rest_get(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    param: Option<&str>,
    body: Option<&str>,
) -> Result<String, String> {
    let mut url = format!("{}{}", server, path);
    if param.is_some() {
        url = format!("{}?{}", url, param.unwrap());
    }

    let body_string = match body {
        Some(b) => b,
        None => "",
    };

    do_rest_request(Method::GET, &url, headers, body_string)
}

pub fn rest_post(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::POST, &url, headers, body)
}

pub fn rest_delete(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::DELETE, &url, headers, body)
}

pub fn rest_put(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::PUT, &url, headers, body)
}

pub fn restapi<F>(server: &str, path: &str, f: F) -> Result<(), String>
where
    F: Fn(String) -> Result<(), String>,
{
    let url = format!("{}{}", server, path);
    let client = reqwest::blocking::Client::new();

    let response = match client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(format!("url get error{}", e.to_string()));
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    f(response.text().unwrap())
}

pub fn check_exist(url: &str) -> bool {
    let client = reqwest::blocking::Client::new();

    let response = match client
        .head(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return false;
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    if response.status().as_str() == "200" {
        return true;
    } else {
        return false;
    }
}

#[cfg(test)]
mod test_exchange {
    use super::*;
    use crate::common::init_debug_log;

    #[test]
    fn test_log_download() {
        init_debug_log();
    }

    #[test]
    fn log_download_temp_test() {
        init_debug_log();
        let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";
        let tmp_dir = tempdir().unwrap();
        let _r = log_download_tmp(url, tmp_dir.path());
    }

    #[test]
    fn log_download_temp_test_bb() {
        init_debug_log();
        //let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";

        let url = "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2020-05-18.csv.gz";

        let tmp_dir = tempdir().unwrap();
        let _r = log_download_tmp(url, tmp_dir.path());
    }

    #[test]
    fn test_rest_api() {
        restapi(
            "https://api.binance.com",
            "/api/v3/trades?symbol=BTCBUSD&limit=1000",
            |s| {
                println!("{}", s);
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn test_rest_get() {
        let s = rest_get(
            "https://api.binance.com",
            "/api/v3/trades?symbol=BTCBUSD&limit=5",
            vec![],
            None,
            None,
        )
        .unwrap();
        println!("{}", s);
    }
}
