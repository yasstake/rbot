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

    println!("file to download: '{}'", fname);

    let fname = tmp_dir.join(fname);
    println!("will be located under: '{:?}'", fname);

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
    // let reader = BufReader::new(content);

    if copy(&mut cursor, &mut target).is_err() {
       return Err(format!("write error"));
    }

    target.flush();

    log::debug!("download size {}", target.metadata().unwrap().len());

    Ok(file_name.to_string())
}

pub fn log_download<F>(url: &str, has_header: bool, mut f: F) -> Result<i64, String>
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

pub fn log_download2<F>(url: &str, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("Downloading ...[{}]", url);

    let result = reqwest::blocking::get(url);

    let response: reqwest::blocking::Response;

    match result {
        Ok(r) => {
            response = r;
        }
        Err(e) => {
            log::debug!("{}", e.to_string());
            return Err(e.to_string());
        }
    }

    let status_code = response.status();
    log::debug!("http status code = {}", status_code);
    if status_code.is_success() == false {
        return Err(format!("HTTP ERROR [{:?}] url={}", status_code, url));
    }

    if url.ends_with("gz") || url.ends_with("GZ") {
        return gzip_log_download(response, has_header, f);
    } else if url.ends_with("zip") || url.ends_with("ZIP") {
        return zip_log_download(response, has_header, f);
    } else {
        log::error!("unknown file suffix {}", url);
        return Err(format!("").to_string());
    }

    // remove tmp file
}

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
        let mut file = zip.by_index(i).unwrap();

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

/* remove async ver
async fn gziped_log_download<F>(url: &str, mut f:F) -> Result<i64, String>
where F: FnMut(&StringRecord) {
    // async で GET
    let result =
        reqwest::get(url).await;

        let response: Response;
        match result {
            Ok(r) => {
                response = r;
            }
            Err(e) => {
                log::debug!("{}", e.to_string());
                return Err(e.to_string());
            }
        }

        let status_code = response.status();
        log::debug!("http status code = {}", status_code);
        if status_code.is_success() == false {
            return Err(format!("HTTP ERROR [{:?}] url={}", status_code, url));
        }


    // 順次解凍しながら表示
    let reader = response
        .bytes_stream()
        .map_err(|e| io::Error::new(ErrorKind::Other, e))
        .into_async_read();
    //let mut lines  = BufReader::new(GzipDecoder::new(BufReader::new(reader)));
    let gz_reader = GzipDecoder::new(BufReader::new(reader));
    let mut csv_reader = csv_async::AsyncReader::from_reader(gz_reader);

    csv_reader.has_headers();
    let mut records = csv_reader.records();

    let mut no_records: i64 = 0;

    while let Some(record) = records.next().await {
        match record {
            Ok(string_record) => {
                no_records += 1;
                f(&string_record);
            },
            Err(e) => {
                log::error!("{}", e.to_string());
                return Err(e.to_string());
            }
        }
    }

    Ok(no_records)
}
 */

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
        log_download_tmp(url, tmp_dir.path()).await;
    }

    #[test]
    fn log_download_temp_test2() {
        init_debug_log();
        let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";
        let tmp_dir = tempdir().unwrap();
        log_download2(url, false, |rec| {});
    }


    /*
        #[tokio::test]
        async fn test_donload() {
            init_debug_log();

            let result = gziped_log_download("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz",
                |rec| {
                    // println!("{}", rec.get(0).unwrap());
                }
            ).await;

            match result {
                Ok(record_no) => {
                    println!("{}", record_no);
                }
                Err(e) => {
                    println!("{:?}", e);
                }
            }
            // println!("{:?}", result);
        }


        // reqwest https://docs.rs/reqwest/latest/reqwest/struct.Response.html
        #[tokio::test]
        async fn download_yahoo() {
            use futures::StreamExt;

            match reqwest::get("https://www.yahoo.co.jp").await {
                Ok(response) => {
                    let mut stream = response.bytes_stream();

                    while let Some(item) = stream.next().await {
                        print!("{:?}", item.unwrap());
                    }
                }
                Err(e) => {
                    panic!("Open URL Error {:?}", e);
                }
            }
        }


        use std::error;
        #[tokio::test]
        async fn main() -> Result<(), Box<dyn error::Error>> {
            use async_compression::futures::bufread::GzipDecoder;
            use futures::{
                io::{self, BufReader, ErrorKind},
                prelude::*,
            };
            let response =
                reqwest::get("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz").await?;
            let reader = response
                .bytes_stream()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))
                .into_async_read();
            let mut lines = BufReader::new(GzipDecoder::new(BufReader::new(reader)));

            loop {
                let mut buf = String::new();

                match lines.read_line(&mut buf).await {
                    Ok(read_size) => {
                        if read_size == 0 {
                            break;
                        }
                        print!("{}", buf);
                    }
                    Err(_e) => {
                        // EOF
                        break;
                    }
                }
            }

            Ok(())
        }

        #[tokio::test]
        async fn stream_ungzip_get() -> Result<(), String> {
            use async_compression::futures::bufread::GzipDecoder;
            use futures::{
                io::{self, BufReader, ErrorKind},
                prelude::*,
            };
            use reqwest::Response;

            // async で GET
            let result =
                reqwest::get("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz").await;

            let response: Response;
            match result {
                Ok(r) => {
                    response = r;
                }
                Err(e) => {
                    return Err(e.to_string());
                }
            }

            // 順次解凍しながら表示
            let reader = response
                .bytes_stream()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))
                .into_async_read();
            let mut lines = BufReader::new(GzipDecoder::new(BufReader::new(reader)));

            loop {
                let mut buf = String::new();

                match lines.read_line(&mut buf).await {
                    Ok(read_size) => {
                        if read_size == 0 {
                            break;
                        }
                        print!("{}", buf); // 表示
                    }
                    Err(_e) => {
                        // EOF
                        break;
                    }
                }
            }

            Ok(())
        }

        #[tokio::test]
        async fn stream_ungzip_get_csv() -> Result<(), String> {
            use async_compression::futures::bufread::GzipDecoder;
            use csv_async;
            use futures::{
                io::{self, BufReader, ErrorKind},
                prelude::*,
            };
            use reqwest::Response;

            // async で GET
            let result =
                reqwest::get("https://public.bybit.com/trading/BTCUSD/BTCUSD2022-11-17.csv.gz").await;

            let response: Response;
            match result {
                Ok(r) => {
                    response = r;
                }
                Err(e) => {
                    return Err(e.to_string());
                }
            }

            // 順次解凍しながら表示
            let reader = response
                .bytes_stream()
                .map_err(|e| io::Error::new(ErrorKind::Other, e))
                .into_async_read();
            //let mut lines  = BufReader::new(GzipDecoder::new(BufReader::new(reader)));
            let gz_reader = GzipDecoder::new(BufReader::new(reader));
            let mut csv_reader = csv_async::AsyncReader::from_reader(gz_reader);

            csv_reader.has_headers();
            let mut records = csv_reader.records();

            while let Some(record) = records.next().await {
                let mut time_us: i64 = 0;
                let mut price: f64 = 0.0;
                let mut size: f64 = 0.0;
                let mut order_side: String = String::new();
                let mut id: String = String::new();

                let record = record.unwrap();
                time_us = record.get(0).unwrap().parse::<i64>().unwrap() * 1_000;
                order_side = record.get(2).unwrap().to_string();
                size = record.get(3).unwrap().parse::<f64>().unwrap();
                price = record.get(4).unwrap().parse::<f64>().unwrap();
                id = record.get(6).unwrap().to_string();
                println!(
                    "time:{}, price:{}, size:{}, order_side:{}, id:{}",
                    time_us,
                    order_side,
                    size,
                    price,
                    id,
                );
            }

            Ok(())
        }
    */
    /* remove for bybit
        // timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
        // 1668728606,BTCUSD,Buy,611,16661.00,ZeroMinusTick,00fcc5e9-d36f-51ae-9906-11f83ed505a7,3.6672468639337374e+06,611,0.036672468639337374

        #[test]
        fn test_parse() {
            parse_log_rec("1668728606,BTCUSD,Buy,611,16661.00,ZeroMinusTick,00fcc5e9-d36f-51ae-9906-11f83ed505a7,3.6672468639337374e+06,611,0.036672468639337374");
        }

        pub fn parse_log_rec(rec: &str) {
            let rec_trim = rec.trim();
            let row = rec_trim.split(","); // カラムに分割

            let mut time_us: i64 = 0;
            let mut price: f64 = 0.0;
            let mut size: f64 = 0.0;
            let mut order_side: String = String::new();
            let mut id: String = String::new();

            // カラム毎の処理
            for (i, col) in row.enumerate() {
                match i {
                    0 => {
                        /*timestamp*/
                        time_us = col.parse::<i64>().unwrap();
                        time_us *= 1_000;
                    }
                    1 => { /* symbol IGNORE */ }
                    2 => {
                        /* side */
                        order_side = col.to_string();
                    }
                    3 => {
                        /* size */
                        size = col.parse::<f64>().unwrap();
                    }
                    4 => {
                        /* price */
                        price = col.parse::<f64>().unwrap();
                    }
                    5 => { /* tickDirection IGNORE */ }
                    6 => {
                        /* trdMatchID */
                        id = col.to_string();
                    }
                    7 => { /* grossValue IGNORE */ }
                    8 => { /* homeNotional IGNORE */ }
                    9 => { /* foreignNotional IGNORE */ }
                    _ => {
                        /* ERROR */
                        panic!("unknon record format");
                    }
                }
            }

            println!(
                "time_us: {}, order_side: {}, price: {}, size: {}, id: {}",
                time_us, order_side, size, price, id
            );
        }

        pub fn parse_log_rec_csv(rec: &str) {
            let rec_trim = rec.trim();
            let row = rec_trim.split(","); // カラムに分割

            let mut time_us: i64 = 0;
            let mut price: f64 = 0.0;
            let mut size: f64 = 0.0;
            let mut order_side: String = String::new();
            let mut id: String = String::new();

            // カラム毎の処理
            for (i, col) in row.enumerate() {
                match i {
                    0 => {
                        /*timestamp*/
                        time_us = col.parse::<i64>().unwrap();
                        time_us *= 1_000;
                    }
                    1 => { /* symbol IGNORE */ }
                    2 => {
                        /* side */
                        order_side = col.to_string();
                    }
                    3 => {
                        /* size */
                        size = col.parse::<f64>().unwrap();
                    }
                    4 => {
                        /* price */
                        price = col.parse::<f64>().unwrap();
                    }
                    5 => { /* tickDirection IGNORE */ }
                    6 => {
                        /* trdMatchID */
                        id = col.to_string();
                    }
                    7 => { /* grossValue IGNORE */ }
                    8 => { /* homeNotional IGNORE */ }
                    9 => { /* foreignNotional IGNORE */ }
                    _ => {
                        /* ERROR */
                        panic!("unknon record format");
                    }
                }
            }

            println!(
                "time_us: {}, order_side: {}, price: {}, size: {}, id: {}",
                time_us, order_side, size, price, id
            );
        }
    */
}
