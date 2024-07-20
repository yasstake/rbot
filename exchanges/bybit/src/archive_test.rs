use std::path::PathBuf;
use anyhow::anyhow;
use polars_io::avro::AvroWriter;
use polars_io::avro::AvroReader;
use polars_io::parquet::read::ParquetReader;
use polars_io::parquet::write::ParquetWriter;
use std::fs::File;
use polars_io::SerWriter;
use polars_io::SerReader;

use polars::prelude::DataFrame;

pub fn save_df_to_avro(df: &mut DataFrame, path: &mut PathBuf) -> anyhow::Result<PathBuf>{
    path.set_extension("avro");
    let p = path.clone();

    let mut file = File::create(path).expect("could not create file");

    AvroWriter::new(&mut file)
        .finish(df)?;

    Ok(p)
}


pub fn load_df_from_avro(path: &PathBuf) -> anyhow::Result<DataFrame>{
    let file = File::open(path).expect("file not found");

    let df = AvroReader::new(file)
            .finish()?;    

    Ok(df)    
}

pub fn save_df_to_parquet(df: &mut DataFrame, path: &mut PathBuf) -> anyhow::Result<PathBuf>{
    path.set_extension("parquet");
    let p = path.clone();

    let mut file = File::create(path).expect("could not create file");

    ParquetWriter::new(&mut file)
        .finish(df)?;

    Ok(p)
}


pub fn load_df_from_parquet(path: &PathBuf) -> anyhow::Result<DataFrame>{
    let file = File::open(path).expect("file not found");

    let df = ParquetReader::new(file)
            .finish()?;    

    Ok(df)    
}


#[cfg(test)]

mod archive_test {
    use std::path::Path;
    use std::path::PathBuf;

    use rbot_lib::common::date_string;
    use rbot_lib::common::init_debug_log;
    use rbot_lib::common::DAYS;
    use rbot_lib::common::FLOOR_DAY;
    use rbot_lib::common::NOW;
    use rbot_lib::db::log_foreach;
    use rbot_lib::db::TradeTableArchive;
    use rbot_lib::net::log_download_tmp;
    use rbot_market::MarketImpl;

    use crate::rest::BybitRestApi;
    use crate::config::BybitConfig;
    use crate::config::BybitServerConfig;
    use crate::Bybit;

    use super::*;


    fn create_archive() -> TradeTableArchive::<BybitRestApi, BybitServerConfig> {
        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let archive = TradeTableArchive::<BybitRestApi, BybitServerConfig>::new(&server_config, &config);

        archive
    }

    #[test]
    fn test_archive_craet() {
        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();
        let _archive = TradeTableArchive::<BybitRestApi, BybitServerConfig>::new(&server_config, &config);
    }


    #[tokio::test]
    async fn test_archive_latest() -> anyhow::Result<()> {
        let mut archive = create_archive();

        init_debug_log();

        log::debug!("first try, access web archive");
        let latest = archive.latest_archive_date().await?;
        log::debug!("date = {}({})", date_string(latest), latest);

        log::debug!("second try, use cached data");
        let latest = archive.latest_archive_date().await?;
        log::debug!("date = {}({})", date_string(latest), latest);

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_to_csv() {
        let mut archive = create_archive();

        init_debug_log();

        let r = archive.archive_to_csv(NOW()-DAYS(2), true, true).await;
        log::debug!("archive_to_csv result={:?}", r);

        let r = archive.archive_to_csv(NOW()-DAYS(2), false, true).await;
        log::debug!("archive_to_csv result={:?}", r);
    }

    #[tokio::test]
    async fn test_extract() -> anyhow::Result<()> {
        use rbot_lib::db::log_convert;

        let url = "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2024-07-16.csv.gz";

        let tmp_dir = Path::new("/tmp");

        let now = NOW();
        println!("start donwload {:?}", url);
        let tmp_file = log_download_tmp(url, tmp_dir).await?;
        println!("end download {:?} {:?}[usec]", tmp_file, NOW() - now);


        let source_file = PathBuf::from(tmp_file);
        let target_file = PathBuf::from("/tmp/log2.gz");

        let now = NOW();
        println!("start convert {:?}->{:?}", source_file, target_file);
        log_convert(source_file.into(), target_file.into(),
            true,
            |l| l+"\n").await?;

            println!("end convert {:?}[usec]", NOW() - now);

        let file = PathBuf::from("/tmp/log2.gz");

        let now = NOW();
        println!("start read file");
        let mut count: i64 = 0;
        log_foreach(file, false, |_line| {
            count += 1;
        }).await?;
        println!("count={}",count);
        println!("end read file {:?}[usec]", NOW() - now);

        Ok(())
    }


    #[test]
    fn test_path_name_and_date() -> anyhow::Result<()> {
        init_debug_log();
        let archive = create_archive();

        let target_date = NOW() - DAYS(1);

        let path = archive.file_path(target_date);

        let date = archive.file_date(&path)?;
    
        log::debug!("{}={:?}", target_date, date);


        assert_eq!(FLOOR_DAY(target_date), date);

        Ok(())
       }

       #[tokio::test]
       async fn test_list_dates() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();

        archive.archive_to_csv(NOW() - DAYS(2), false, true).await?;
        archive.archive_to_csv(NOW() - DAYS(3), false, true).await?;
        archive.archive_to_csv(NOW() - DAYS(10), false, true).await?;

        log::debug!("start={:?}({:?})", archive.start_time(), date_string(archive.start_time()));
        log::debug!("end={:?}({:?})", archive.end_time(), date_string(archive.end_time()));

        let dates = archive.list_dates()?;

        log::debug!("data dates = {:?}", dates);

        Ok(())
       }

       #[tokio::test]
       async fn test_download() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();
        log::debug!("start={:?}({:?})", archive.start_time(), date_string(archive.start_time()));
        log::debug!("end={:?}({:?})", archive.end_time(), date_string(archive.end_time()));

        log::debug!("download first");

        archive.download(4, false, true).await?;
        log::debug!("start={:?}({:?})", archive.start_time(), date_string(archive.start_time()));
        log::debug!("end={:?}({:?})", archive.end_time(), date_string(archive.end_time()));

        log::debug!("download with cache");

        archive.download(7, false, true).await?;
        log::debug!("start={:?}({:?})", archive.start_time(), date_string(archive.start_time()));
        log::debug!("end={:?}({:?})", archive.end_time(), date_string(archive.end_time()));


        Ok(())
       }

       #[tokio::test]
       async fn test_load_df_and_save_avro() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();

        archive.download(2, false, true).await?;

        log::debug!("start={:?}({:?})", archive.start_time(), date_string(archive.start_time()));
        log::debug!("end={:?}({:?})", archive.end_time(), date_string(archive.end_time()));

        let mut df = archive.load_df(NOW()-DAYS(2))?;
        log::debug!("{:?}", df);

        let now = NOW();
        log::debug!("start save avro");
        save_df_to_avro(&mut df, &mut PathBuf::from("/tmp/BTCUSDT"))?;
        log::debug!("done {}[usec]", NOW() - now);

        let now = NOW();
        log::debug!("start load avro");
        let mut df = load_df_from_avro(&mut PathBuf::from("/tmp/BTCUSDT.avro"))?;
        log::debug!("done {}[usec]", NOW() - now);
        log::debug!("{:?}", df);


        let now = NOW();
        log::debug!("start save parquet");
        save_df_to_parquet(&mut df, &mut PathBuf::from("/tmp/BTCUSDT"))?;
        log::debug!("done {}[usec]", NOW() - now);

        let now = NOW();
        log::debug!("start load parquet");
        let df = load_df_from_parquet(&mut PathBuf::from("/tmp/BTCUSDT.parquet"));
        log::debug!("done {}[usec]", NOW() - now);
        log::debug!("{:?}", df);




        Ok(())
       }

       #[tokio::test]
       async fn test_load_df() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();

        archive.download(2, false, true).await?;

        log::debug!("start={:?}({:?})", archive.start_time(), date_string(archive.start_time()));
        log::debug!("end={:?}({:?})", archive.end_time(), date_string(archive.end_time()));

        let df = archive.load_df(NOW()-DAYS(2))?;
        log::debug!("{:?}", df);

        let df = archive.load_df(NOW()-DAYS(100))?;
        log::debug!("{:?}", df);

        let df = archive.load_df(NOW()-DAYS(0))?;
        log::debug!("{:?}", df);



            Ok(())
       }


    #[test]
    fn test_select_dates() {
        init_debug_log();
        let archive = create_archive();

        let dates = archive.select_dates(0, 0);
        log::debug!("{:?}", dates);

        let dates = archive.select_dates(0, NOW() - DAYS(2));
        log::debug!("{:?}", dates);

        let dates = archive.select_dates(NOW() - DAYS(3), NOW() - DAYS(2));
        log::debug!("{:?}", dates);
    }

    #[test]
    fn test_foe_each_rec_perf() -> anyhow::Result<()>{
        init_debug_log();
        let archive = create_archive();

        let now = NOW();

        let count = archive.for_each_record(0, 0, 
            &mut |trade|{Ok(())}
        )?;

        log::debug!("process count= {}, {}[usec]", count, NOW() - now);

        log::debug!("自力ループによる読み込み {}[rec]  {}[msec]", count, (NOW()-now)/1_000 as i64);

        Ok(())
    } 

    #[test]
    fn test_select_df_perf() -> anyhow::Result<()> {
        init_debug_log();
        let archive = create_archive();

        let now = NOW();

        let df = archive.select_df(0, 0)?;

        log::debug!("{:?}", df);
        log::debug!("{:?}", df.shape());
        log::debug!("{:?}", NOW() - now);

        log::debug!("Polarsによる読み込み {}[rec]  {}[msec]", df.shape().0, (NOW()-now)/1_000 as i64);


        Ok(())
    }

    #[test]
    fn test_select_db_perf() -> anyhow::Result<()> {
        let mut market = Bybit::new(false).open_market(&BybitConfig::BTCUSDT());
        init_debug_log();

        market.download_archives(7, false, true, false);


        let now = NOW();

        let mut count = 0;
        market.for_each_record(0, 0, &mut |trade|{
            count += 1;
            Ok(())
        });

        log::debug!("Sqliteによる読み込み {}[rec]  {}[msec]", count, (NOW()-now)/1_000 as i64);


        Ok(())
    }
}