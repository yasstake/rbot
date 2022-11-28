use crate::common::order::{Trade, TimeChunk};
use crate::common::time::{time_string, to_seconds, MicroSec, DAYS, HHMM, NOW, MICRO_SECOND};
use log;
use std::thread::sleep;
use std::time::Duration;
use crate::exchange::ftx::message::FtxTradeMessage;

const FTX_REST_ENDPOINT: &str = "https://ftx.com/api";


// TODO: RESTAPIのエラー処理（取引所エラー）対応。

/*
pub fn download_trade_ndays_channel(
    market_name: &str,
    ndays: i64,
    force: bool,
    tx: &mut Sender<Vec<Trade>>,
) {
    log::debug!("download_trade_ndays {}", ndays);
    let start_time = NOW() - DAYS(ndays) - HHMM(0, 10);
    let mut end_time = NOW() + HHMM(0, 5); // 5分後を指定し最新を取得。

    loop {
        let timer_start = NOW();

        log::debug!("download trade to: {}", time_string(end_time));

        let trades = download_trade(market_name, start_time, end_time);
        let trade_len = trades.len();
        end_time = trades[trade_len - 1].time;

        tx.send(trades);

        let lap_time = NOW() - timer_start;
        log::debug!("{} trades / {} [us]", trade_len, lap_time);

        if trade_len <= 100 || end_time <= start_time + 2 {
            break;
        }

        sleep(Duration::from_millis(10));
    }
}

pub fn download_trade_ndays(market_name: &str, ndays: i64, db: &mut TradeTable) {
    log::debug!("download_trade_ndays {}", ndays);
    let start_time = NOW() - DAYS(ndays) - HHMM(0, 10);
    let mut end_time = NOW() + HHMM(0, 5); // 5分後を指定し最新を取得。

    loop {
        let timer_start = NOW();

        log::debug!("download trade to: {}", time_string(end_time));

        let trades = download_trade(market_name, start_time, end_time);
        let trade_len = trades.len();
        end_time = trades[trade_len - 1].time;

        db.insert_records(&trades);

        let lap_time = NOW() - timer_start;
        log::debug!("{} trades / {} [us]", trade_len, lap_time);

        if trade_len <= 100 || end_time <= start_time + 2 {
            break;
        }

        sleep(Duration::from_millis(10));
    }
}

pub fn download_trade_call<F>(market_name: &str, ndays: i32, mut f: F)
where
    F: FnMut(Vec<Trade>),
{
    log::debug!("download_trade_ndays {}", ndays);
    let start_time = NOW() - DAYS(ndays as i64) - HHMM(0, 10);
    let mut end_time = NOW() + HHMM(0, 5); // 5分後を指定し最新を取得。

    loop {
        let timer_start = NOW();

        log::debug!(
            "download trade to: {}-{}({})",
            time_string(start_time),
            time_string(end_time),
            end_time
        );

        let mut trades = download_trade(market_name, start_time, end_time);
        let trade_len = trades.len();
        end_time = trades[trade_len - 1].time;

        log::debug!("downloaed from {}  / to {}", trades[0].time, end_time);

        f(trades);

        let lap_time = NOW() - timer_start;
        log::debug!("{} trades / {} [us]", trade_len, lap_time);

        if trade_len <= 100 || end_time <= start_time + 2 {
            break;
        }

        sleep(Duration::from_millis(10));
    }
}
*/



pub fn download_trade_callback_ndays<F>(
    market_name: &str,
    ndays: i32,
    f: F) where F: FnMut(Vec<Trade>),
{
    let start_time = NOW() - DAYS(ndays as i64) - HHMM(0, 10);
    let end_time = NOW() + HHMM(0, 5); // 5分後を指定し最新を取得。
    
    log::debug!("download_trade_ndays {} / {} -> {}", ndays, time_string(start_time), time_string(end_time));

    download_trade_callback(market_name, start_time, end_time, f)
}

pub fn download_trade_chunks_callback<F>(
    market_name:& str, 
    chunk: &Vec<TimeChunk>,
    mut f: F,
)where
    F: FnMut(Vec<Trade>)
{
    for ch in chunk {
        download_trade_callback(market_name, ch.start, ch.end, &mut f);
    }
}

pub fn download_trade_callback<F>(
    market_name: &str,
    start_time: MicroSec,
    mut end_time: MicroSec,
    mut f: F,
) where
F: FnMut(Vec<Trade>),
{
    log::debug!("download trade call back start_time {} -> end_time {}", time_string(start_time), time_string(end_time));

    loop {
        let timer_start = NOW();

        log::debug!(
            "download trade to: {}-{}({})",
            time_string(start_time),
            time_string(end_time),
            end_time
        );
        let trades = download_trade(market_name, start_time, end_time);
        let trade_len = trades.len();
        end_time = trades[trade_len - 1].time;

        log::debug!("download time from {} {} / to {} {}", end_time, time_string(end_time), trades[0].time, time_string(trades[0].time));

        f(trades);

        let lap_time = NOW() - timer_start;
        log::debug!("{} trades / {} [us]", trade_len, lap_time);

        if trade_len <= 100 ||  end_time <= start_time + (10 * MICRO_SECOND) {
            break;
        }

        sleep(Duration::from_millis(1));
    }
}


/// TODO: エラーハンドリング（JSONエラー/503エラーの場合、現在はPanicしてしまう）
/// start_time, to_timeは秒単位のため開始は切り捨て、終了は切り上げする。
/// そのため、重複が発生するのであとでIDで重複削除する必要がある。
/// returns reverse order in time stamp
pub fn download_trade(
    market_name: &str,
    from_microsec: MicroSec,
    to_microsec: MicroSec,
) -> Vec<Trade> {
    let start_sec = to_seconds(from_microsec) as i64; // round down
    let end_sec = to_seconds(to_microsec) as i64 + 1; // round up

    let url = format!(
        "{}/markets/{}/trades?start_time={}&end_time={}",
        FTX_REST_ENDPOINT, market_name, start_sec, end_sec
    );
    log::debug!("{} / {} -> {}", url, time_string(start_sec*MICRO_SECOND), time_string(end_sec*MICRO_SECOND));

    let response = reqwest::blocking::get(url);

    return match response {
        Ok(response) => match response.text() {
            Ok(res) => match FtxTradeMessage::from_str(res.as_str()) {
                Ok(mut message) => {
                    let mut trades = message.get_trades();
                    trades.sort_by(|a, b| b.time.cmp(&a.time));

                    trades    // Ok!!
                },
                Err(e) => {
                    log::warn!("log history format(json) error = {}/{}", e, res);
                    vec![]
                }
            },
            Err(e) => {
                log::warn!("log history format(json) error = {}", e);
                vec![]
            }
        },
        Err(e) => {
            log::warn!("download history error = {:?}", e);
            vec![]
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///   Test Suite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod test_ftx_client {
    const BTCMARKET: &str = "BTC-PERP";

    use super::*;
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use crate::common::init_log;
    use crate::common::time::{DAYS, NOW};
    use crate::db::sqlite::TradeTable;
    use crate::fs::db_full_path;
    use crate::time_string;

    #[test]
    pub fn test_download_trade_callback() {
        init_log();
        let to_time = NOW() + HHMM(0, 5);
        let from_time = to_time - HHMM(10, 0);

        download_trade_callback (BTCMARKET, from_time, to_time, |trade| {
            log::debug!("{:?} -> {:?}", trade[trade.len()-1].time, trade[0].time);
        });
    }

    #[test]
    pub fn test_download_trade_callback_ndays() {

        let mut writer = BufWriter::new(File::create("./ftx.csv").unwrap());

        download_trade_callback_ndays(
            BTCMARKET,
            1,
            |trade| {
                for t in trade {
                    // println!("{}", t.to_csv())
                    let _ = writer.write_all(t.to_csv().as_bytes());
                }
            });
        
            let _= writer.flush();
    }


    #[test]
    fn test_call_back_db() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        db.create_table_if_not_exists();

        let to_time = NOW() + HHMM(0, 5);
        let from_time = to_time - HHMM(20, 0);

        download_trade_callback(BTCMARKET, from_time, to_time, |trade| {
            let _ = db.insert_records(&trade);
        });
    }


    #[test]
    fn test_download_trade() {
        init_log();
        let to_time = NOW() + HHMM(0, 5);
        let from_time = to_time - DAYS(10);

        let trades = download_trade(BTCMARKET, from_time, to_time);
        log::debug!(
            "FROM: {:?} {:?}",
            trades[trades.len() - 1].time,
            time_string(trades[trades.len() - 1].time)
        );
        log::debug!(
            "TO  : {:?} {:?}",
            trades[0].time,
            time_string(trades[0].time)
        );
        log::debug!("Trade len = {:?}", trades.len());
        log::debug!("FIRST= {:?}/{:?}", trades[0], time_string(trades[0].time));
        log::debug!("LAST = {:?}{:?}", trades[trades.len() - 1], time_string(trades[trades.len() - 1].time));

        let to_time = trades[trades.len() - 1].time;
        let trades = download_trade(BTCMARKET, from_time, to_time);
        log::debug!(
            "FROM: {:?} {:?}",
            trades[trades.len() - 1].time,
            time_string(trades[trades.len() - 1].time)
        );
        log::debug!(
            "TO  : {:?} {:?}",
            trades[0].time,
            time_string(trades[0].time)
        );
        log::debug!("Trade len = {:?}", trades.len());
        log::debug!("FIRST= {:?}/{:?}", trades[0], time_string(trades[0].time));
        log::debug!("LAST = {:?}{:?}", trades[trades.len() - 1], time_string(trades[trades.len() - 1].time));

        let to_time = trades[trades.len() - 1].time;
        let trades = download_trade(BTCMARKET, from_time, to_time);
        log::debug!(
            "FROM: {:?} {:?}",
            trades[trades.len() - 1].time,
            time_string(trades[trades.len() - 1].time)
        );
        log::debug!(
            "TO  : {:?} {:?}",
            trades[0].time,
            time_string(trades[0].time)
        );
        log::debug!("Trade len = {:?}", trades.len());
        log::debug!("FIRST= {:?}/{:?}", trades[0], time_string(trades[0].time));
        log::debug!("LAST = {:?}{:?}", trades[trades.len() - 1], time_string(trades[trades.len() - 1].time));
    }

/*

    #[test]
    fn store_db() {
        init_log();

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        db.create_table_if_not_exists();

        download_trade_ndays(BTCMARKET, 1, &mut db);
    }

    #[test]
    fn call_back() {
        init_log();

        download_trade_call(BTCMARKET, 0, |trade| println!("{:?}", trade));
    }


    #[test]
    fn call_back_db_thread() {
        init_log();

        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();

        // Txがあるかぎり、rxはエラーを出さない。
        //let tx2 = tx.clone();

        let handle = thread::spawn(move || {
            download_trade_call(BTCMARKET, 1, |trade| {
                tx.send(trade);
            });
        });

        let db_name = db_full_path("FTX", "BTC-PERP");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        db.create_table_if_not_exists();

        loop {
            match rx.recv() {
                Ok(trades) => db.insert_records(&trades).unwrap(),
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
            }
        }
    }
    */
}

