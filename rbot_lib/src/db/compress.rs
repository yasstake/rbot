use anyhow::Context;
use anyhow::anyhow;
use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use async_std::io::BufReader;
use async_std::stream::StreamExt;
use futures::TryStreamExt;
use std::io::Cursor;
use url::Url;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, Lines};
use reqwest::Client;

//use futures::io::AsyncBufReadExt;
//use futures::io::AsyncWriteExt;
//use std::io::BufReader;
//use tokio::fs::File;
//use tokio::io::AsyncReadExt;
//use tokio::io::BufWriter;



pub async fn log_convert<F>(source_file: PathBuf, target_file: PathBuf, has_header: bool, f: F) -> anyhow::Result<()>
where
    F: Fn(String) -> String
{
    let source_file = tokio::fs::File::open(source_file).await?;
    let target_file = tokio::fs::File::create(target_file).await?;

    let reader = tokio::io::BufReader::new(source_file);
    let writer = tokio::io::BufWriter::new(target_file);

    let decoder = GzipDecoder::new(reader);
    let decoder = tokio::io::BufReader::new(decoder);

    let encoder = GzipEncoder::new(writer);
    let mut encoder = tokio::io::BufWriter::new(encoder);

    let mut lines = decoder.lines();

    if has_header {
        let header = lines.next_line().await?;
        log::debug!("header: {:?}", header);
    }

    while let Some(mut line) = lines.next_line().await? {
        line = f(line);
        encoder.write_all(line.as_bytes()).await?;
        // println!("length = {}", line.len());
    }

    encoder.flush().await;
    encoder.shutdown().await?;

    Ok(())
}

pub async fn log_foreach<F>(source_file: PathBuf, has_header: bool, mut f: F) -> anyhow::Result<()>
where
    F: FnMut(String)
{
    let source_file = tokio::fs::File::open(source_file).await?;

    let reader = tokio::io::BufReader::new(source_file);
    let decoder = GzipDecoder::new(reader);
    let decoder = tokio::io::BufReader::new(decoder);

    let mut lines = decoder.lines();

    if has_header {
        let header = lines.next_line().await?;
        log::debug!("header: {:?}", header);
    }

    while let Some(line) = lines.next_line().await? {
        f(line);
    }

    Ok(())
}

