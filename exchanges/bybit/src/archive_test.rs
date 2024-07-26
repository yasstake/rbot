
#[cfg(test)]

mod archive_test {
    use std::path::PathBuf;
    use std::str::FromStr;

    use rbot_lib::common::date_string;
    use rbot_lib::common::init_debug_log;
    use rbot_lib::common::DAYS;
    use rbot_lib::common::NOW;
    use rbot_lib::db::log_download_tmp;
    use rbot_lib::db::TradeArchive;
    use rbot_lib::net::RestApi;
    
    use crate::config::BybitConfig;
    use crate::config::BybitServerConfig;
    use crate::rest::BybitRestApi;
    

    fn create_archive() -> TradeArchive {
        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let archive =
            TradeArchive::new(&config, server_config.production);

        archive
    }


    #[tokio::test]
    async fn test_archive_latest() -> anyhow::Result<()> {
        let mut archive = create_archive();

        init_debug_log();
        let server_config = BybitServerConfig::new(true);
        let api = BybitRestApi::new(&server_config);

        log::debug!("first try, access web archive");
        let latest = archive.latest_archive_date(&api).await?;
        log::debug!("date = {}({})", date_string(latest), latest);

        log::debug!("second try, use cached data");
        let latest = archive.latest_archive_date(&api).await?;
        log::debug!("date = {}({})", date_string(latest), latest);


        Ok(())
    }


    #[test]
    fn test_foreach_count() -> anyhow::Result<()> {
        let archive = create_archive();
        init_debug_log();

        let mut rec: i64 = 0;
        let now = NOW();
        let count = archive.foreach(0, 0, &mut |_trade|{
            rec += 1;
            Ok(())
        })?;

        log::debug!("result={} loop count={} {}[msec]", count, rec, NOW()-now);

        Ok(())
    }

    #[tokio::test]
    async fn test_test_download_tmp() -> anyhow::Result<()> {
        init_debug_log();
        let server = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let api = BybitRestApi::new(&server);

        let url = api.history_web_url(&config, NOW() - DAYS(2));
        log::debug!("url={:?}", url);

        let tmp_dir = PathBuf::from_str("/tmp/")?;

        let file = log_download_tmp(&url, &tmp_dir, |_, _|{}).await?;    

        log::debug!("download complete {:?}", file);

        Ok(())
    }

    #[tokio::test]
    async fn test_web_archive_to_parquet() {
        let mut archive = create_archive();

        init_debug_log();
        let server_config = BybitServerConfig::new(true);
        let api = BybitRestApi::new(&server_config);


        archive.web_archive_to_parquet(&api, NOW() - DAYS(2), false, true, |_, _|{}).await.unwrap();
    }

    #[test]
    fn test_load_cache_df() -> anyhow::Result<()> {
        let archive = create_archive();
        init_debug_log();


        let df = archive.load_cache_df(NOW()-DAYS(2))?;

        log::debug!("{:?}", df);

        Ok(())
    }

    #[test]
    fn test_select_cache_df() -> anyhow::Result<()> {
        let archive = create_archive();
        init_debug_log();

        let df = archive.select_cachedf(0, 0)?;

        log::debug!("{:?}", df);

        Ok(())
    }



    #[tokio::test]
    async fn test_list_dates() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();

        let server_config = BybitServerConfig::new(true);
        let api = BybitRestApi::new(&server_config);


        archive.web_archive_to_parquet(&api, NOW() - DAYS(2), false, true, |_, _|{}).await?;
        archive.web_archive_to_parquet(&api, NOW() - DAYS(3), false, true, |_, _|{}).await?;
        archive
            .web_archive_to_parquet(&api, NOW() - DAYS(10), false, true, |_, _|{})
            .await?;

        log::debug!(
            "start={:?}({:?})",
            archive.start_time(),
            date_string(archive.start_time()?)
        );
        log::debug!(
            "end={:?}({:?})",
            archive.end_time(),
            date_string(archive.end_time()?)
        );

        let dates = archive.list_dates()?;

        log::debug!("data dates = {:?}", dates);

        Ok(())
    }

    #[tokio::test]
    async fn test_download() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();
        log::debug!(
            "start={:?}({:?})",
            archive.start_time(),
            date_string(archive.start_time()?)
        );
        log::debug!(
            "end={:?}({:?})",
            archive.end_time(),
            date_string(archive.end_time()?)
        );

        log::debug!("download first");
        let server_config = BybitServerConfig::new(true);
        let api = BybitRestApi::new(&server_config);


        archive.download(&api, 4, false, true).await?;
        log::debug!(
            "start={:?}({:?})",
            archive.start_time(),
            date_string(archive.start_time()?)
        );
        log::debug!(
            "end={:?}({:?})",
            archive.end_time(),
            date_string(archive.end_time()?)
        );

        log::debug!("download with cache");

        archive.download(&api, 7, false, true).await?;
        log::debug!(
            "start={:?}({:?})",
            archive.start_time(),
            date_string(archive.start_time()?)
        );
        log::debug!(
            "end={:?}({:?})",
            archive.end_time(),
            date_string(archive.end_time()?)
        );

        Ok(())
    }


    #[tokio::test]
    async fn test_load_df() -> anyhow::Result<()> {
        init_debug_log();
        let mut archive = create_archive();
        let server_config = BybitServerConfig::new(true);
        let api = BybitRestApi::new(&server_config);

        archive.download(&api, 2, false, true).await?;

        log::debug!(
            "start={:?}({:?})",
            archive.start_time(),
            date_string(archive.start_time()?)
        );
        log::debug!(
            "end={:?}({:?})",
            archive.end_time(),
            date_string(archive.end_time()?)
        );

        let df = archive.load_cache_df(NOW() - DAYS(2))?;
        log::debug!("{:?}", df);

        let df = archive.load_cache_df(NOW() - DAYS(100))?;
        log::debug!("{:?}", df);

        let df = archive.load_cache_df(NOW() - DAYS(0))?;
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
    fn test_select_df_perf() -> anyhow::Result<()> {
        init_debug_log();
        let archive = create_archive();

        let now = NOW();

        let df = archive.select_cachedf(0, 0)?;

        log::debug!("{:?}", df);
        log::debug!("{:?}", df.shape());
        log::debug!("{:?}", NOW() - now);

        log::debug!(
            "Polarsによる読み込み {}[rec]  {}[msec]",
            df.shape().0,
            (NOW() - now) / 1_000 as i64
        );

        Ok(())
    }

}
