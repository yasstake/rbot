// Copyright(c) 2022. yasstake. All rights reserved.
// ABSOLUTELY NO WARRANTY.

#![allow(non_snake_case)]

use std::str::FromStr;

use chrono::{DateTime, Datelike as _, NaiveDate, NaiveDateTime, TimeZone, Utc};
use pyo3::prelude::*;

use anyhow::anyhow;

pub const MICRO_SECOND: i64 = 1_000_000;
pub const NANO_SECOND: i64 = 1_000_000_000;

// Timestamp scale for system wide.(Micro Sec(10^-6 is default)
pub type MicroSec = i64;

pub fn msec_to_microsec(t: i64) -> MicroSec {
    return (t as i64) * 1_000;
}

pub fn microsec_to_sec(t: MicroSec) -> i64 {
    t / MICRO_SECOND
}

pub fn to_seconds(microsecond: MicroSec) -> f64 {
    return (microsecond as f64) / (MICRO_SECOND as f64);
}

pub fn to_naive_datetime(microsecond: MicroSec) -> DateTime<Utc> {
    let sec = microsecond / MICRO_SECOND;
    let nano = ((microsecond % MICRO_SECOND) * 1_000) as u32;
    //let datetime = NaiveDateTime::from_timestamp_opt(sec, nano);
    let datetime = DateTime::from_timestamp(sec, nano);    

    return datetime.unwrap();
}

#[pyfunction]
pub fn FLOOR_SEC(microsecond: MicroSec, unit_sec: i64) -> MicroSec {
    let unit_sec_micro = SEC(unit_sec);

    let floor = ((microsecond / unit_sec_micro) as i64) * unit_sec_micro;

    return floor;
}

pub fn FLOOR_DAY(timestamp: MicroSec) -> MicroSec {
    return FLOOR_SEC(timestamp, 24 * 60 * 60);
}

pub fn FLOOR_HOUR(timestamp: MicroSec) -> MicroSec {
    return FLOOR_SEC(timestamp, 60 * 60);
}

pub fn TODAY() -> MicroSec {
    return FLOOR_DAY(NOW());
}

pub fn CEIL(microsecond: MicroSec, unit_sec: i64) -> MicroSec {
    let unit_sec_micro = SEC(unit_sec);

    let floor = ((microsecond + unit_sec_micro - 1) / unit_sec_micro as i64) * unit_sec_micro;

    return floor;
}

#[pyfunction]
pub fn time_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    return datetime.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
}

#[pyfunction]
pub fn short_time_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    return datetime.format("%Y-%m-%dT%H:%M:%S").to_string();
}

#[pyfunction]
pub fn hour_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    return datetime.format("%H").to_string();
}

#[pyfunction]
pub fn min_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    return datetime.format("%M").to_string();
}

/// convert time to YYYYMMDD format
#[pyfunction]
pub fn date_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    return datetime.format("%Y%m%d").to_string();
}

#[pyfunction]
pub fn date_time_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    return datetime.format("%Y/%m/%dT%H:%M").to_string();
}


///  convert YYYYMMDD format date to MicroSec,
///  return 0 in error.
#[pyfunction]
pub fn parse_date(date: &str) -> anyhow::Result<MicroSec> {
    if date.len() != 8 {
        return Err(anyhow!("illeagal format {:?}", date));
    }

    let yyyy: i32 = date[0..4].parse()?;
    let mm: u32 = date[4..6].parse()?;
    let dd: u32 = date[6..8].parse()?;

    log::debug!{"{} -> {} {} {}", date, yyyy, mm, dd};

    let date = Utc.with_ymd_and_hms(yyyy, mm, dd, 0, 0, 0).unwrap();

    Ok(date.timestamp_micros() as MicroSec)
}

#[pyfunction]
pub fn parse_time(t: &str) -> MicroSec {
    let datetime = DateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.6f%z");

    return datetime.unwrap().timestamp_micros();
}

#[pyfunction]
pub fn DAYS(days: i64) -> MicroSec {
    return (24 * 60 * 60 * MICRO_SECOND * days) as MicroSec;
}

#[pyfunction]
pub fn DAYS_BEFORE(days: i64) -> MicroSec {
    return NOW() - DAYS(days);
}

#[pyfunction]
pub fn HHMM(hh: i64, mm: i64) -> MicroSec {
    return ((hh * 60 * 60) * MICRO_SECOND + MIN(mm)) as MicroSec;
}

#[pyfunction]
pub fn MIN(min: i64) -> MicroSec {
    return min * MICRO_SECOND * 60;
}

#[pyfunction]
pub fn SEC(sec: i64) -> MicroSec {
    return sec * MICRO_SECOND as MicroSec;
}

pub fn split_yyyymmdd(t: MicroSec) -> (i64, i64, i64)
{
    let timestamp = to_naive_datetime(t);

    let yyyy = timestamp.year() as i64;
    let mm = timestamp.month() as i64;
    let dd = timestamp.day() as i64;

    (yyyy, mm, dd)
}

///
/// 現在時刻を返す(Microsecond)
/// ```
/// println!("{:?}", NOW());
/// ```

#[pyfunction]
pub fn NOW() -> MicroSec {
    return Utc::now().timestamp_micros();
}

#[cfg(test)]
mod time_test {
    use crate::common::init_debug_log;

    use super::*;
    #[test]
    fn test_floor() {
        assert_eq!(
            FLOOR_SEC(1_000_000 - 1, 1),
            parse_time("1970-01-01T00:00:00.000000+00:00")
        );
        assert_eq!(
            FLOOR_SEC(1_000_000, 1),
            parse_time("1970-01-01T00:00:01.000000+00:00")
        );

        assert_eq!(
            FLOOR_SEC(MIN(1) + 1, 10),
            parse_time("1970-01-01T00:01:00.000000+00:00")
        );
        assert_eq!(
            FLOOR_SEC(DAYS(1) + MIN(1) + 1, 60 * 10),
            parse_time("1970-01-02T00:00:00.000000+00:00")
        );
    }

    #[test]
    fn test_to_str() {
        assert_eq!(time_string(0), "1970-01-01T00:00:00.000000");
        assert_eq!(time_string(1), "1970-01-01T00:00:00.000001");
        assert_eq!(time_string(1_000_001), "1970-01-01T00:00:01.000001");
    }

    #[test]
    fn test_short_timestring() {
        assert_eq!(short_time_string(0), "1970-01-01T00:00:00");
    }

    // https://rust-lang-nursery.github.io/rust-cookbook/datetime/parse.html
    #[test]
    fn test_parse_time() {
        const TIME1: &str = "2022-10-22T14:22:43.407735+00:00";
        let r = parse_time(TIME1);
        println!("{:?}", r);

        assert_eq!(1_000_001, parse_time("1970-01-01T00:00:01.000001+00:00"));
    }

    #[test]
    fn test_days() {
        assert_eq!(DAYS(1), parse_time("1970-01-02T00:00:00.000000+00:00"));
        assert_eq!(HHMM(1, 1), parse_time("1970-01-01T01:01:00.000000+00:00"));
        assert_eq!(MIN(2), parse_time("1970-01-01T00:02:00.000000+00:00"));
        assert_eq!(SEC(3), parse_time("1970-01-01T00:00:03.000000+00:00"));
    }

    #[test]
    fn test_print_now() {
        let now = NOW();
        println!("{:?} {:?}", now, time_string(now));
    }

    #[test]
    fn test_floor2() {
        assert_eq!(0, FLOOR_SEC(999_999, 1));
        assert_eq!(1_000_000, FLOOR_SEC(1_000_000, 1));
        assert_eq!(1_000_000, FLOOR_SEC(1_000_111, 1));
        assert_eq!(10_000_000, FLOOR_SEC(10_123_111, 10));
        assert_eq!(10_000_000, FLOOR_SEC(19_123_111, 10));
        assert_eq!(20_000_000, FLOOR_SEC(29_123_111, 10));
    }

    #[test]
    fn test_ceil() {
        assert_eq!(1_000_000, CEIL(999_999, 1));
        assert_eq!(1_000_000, CEIL(1_000_000, 1));
        assert_eq!(2_000_000, CEIL(1_000_001, 1));
    }

    #[test]
    fn test_yymmdd() -> anyhow::Result<()>{
        init_debug_log();

        assert_eq!(0,       parse_date("19700101")?);
        assert_eq!(DAYS(9), parse_date("19700110")?);
    
        Ok(())
    }

    #[test]
    fn test_split_yyyymmdd() {
        let (yyyy, mm, dd)  = split_yyyymmdd(0);
        assert_eq!(yyyy, 1970);
        assert_eq!(mm, 1);
        assert_eq!(dd, 1);
    }
}
