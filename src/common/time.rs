use chrono::{DateTime, NaiveDateTime, Utc};
use pyo3::prelude::*;

pub const MICRO_SECOND: i64 = 1_000_000;
pub const NANO_SECOND: i64 = 1_000_000_000;

// Timestamp scale for system wide.(Nano Sec is default)
pub type MicroSec = i64;

pub fn to_seconds(microsecond: MicroSec) -> f64 {
    return (microsecond as f64) / (MICRO_SECOND as f64);
}

pub fn to_naive_datetime(microsecond: MicroSec) -> NaiveDateTime {
    let sec = microsecond / MICRO_SECOND;
    let nano = ((microsecond % MICRO_SECOND) * 1_000) as u32;
    let datetime = NaiveDateTime::from_timestamp(sec, nano);

    return datetime;
}

#[allow(non_snake_case)]
pub fn FLOOR(microsecond: MicroSec, unit_sec: i64) -> MicroSec {
    let unit_sec_micro = SEC(unit_sec);

    let floor = ((microsecond / unit_sec_micro) as i64) * unit_sec_micro;

    return floor;
}

pub fn FLOOR_DAY(timestamp: MicroSec) -> MicroSec {
    return FLOOR(timestamp, 24 * 60 * 60);
}

#[allow(non_snake_case)]
pub fn CEIL(microsecond: MicroSec, unit_sec: i64) -> MicroSec {
    let unit_sec_micro = SEC(unit_sec);

    let floor = ((microsecond + unit_sec_micro - 1) / unit_sec_micro as i64) * unit_sec_micro;

    return floor;
}

#[pyfunction]
pub fn time_string(t: MicroSec) -> String {
    let datetime = to_naive_datetime(t);

    // return datetime.format("%Y-%m-%d-%H:%M:%S.").to_string() + format!("{:06}", nano).as_str();
    return datetime.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
}

#[pyfunction]
pub fn parse_time(t: &str) -> MicroSec {
    let datetime = DateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.6f%z");

    return datetime.unwrap().timestamp_micros();
}

#[allow(non_snake_case)]
#[pyfunction]
pub fn DAYS(days: i64) -> MicroSec {
    return (24 * 60 * 60 * MICRO_SECOND * days) as MicroSec;
}

#[allow(non_snake_case)]
#[pyfunction]
pub fn HHMM(hh: i64, mm: i64) -> MicroSec {
    return ((hh * 60 * 60) * MICRO_SECOND + MIN(mm)) as MicroSec;
}

#[pyfunction]
#[allow(non_snake_case)]
pub fn MIN(sec: i64) -> MicroSec {
    return sec * MICRO_SECOND * 60;
}

#[pyfunction]
#[allow(non_snake_case)]
pub fn SEC(sec: i64) -> MicroSec {
    return sec * MICRO_SECOND as MicroSec;
}

///
/// 現在時刻を返す(Microsecond)
/// ```
/// println!("{:?}", NOW());
/// ```
#[allow(non_snake_case)]
#[pyfunction]
pub fn NOW() -> MicroSec {
    return Utc::now().timestamp_micros();
}

#[cfg(test)]
mod time_test {
    use super::*;
    #[test]
    fn test_floor() {
        assert_eq!(
            FLOOR(1_000_000 - 1, 1),
            parse_time("1970-01-01T00:00:00.000000+00:00")
        );
        assert_eq!(
            FLOOR(1_000_000, 1),
            parse_time("1970-01-01T00:00:01.000000+00:00")
        );

        assert_eq!(
            FLOOR(MIN(1) + 1, 10),
            parse_time("1970-01-01T00:01:00.000000+00:00")
        );
        assert_eq!(
            FLOOR(DAYS(1) + MIN(1) + 1, 60 * 10),
            parse_time("1970-01-02T00:00:00.000000+00:00")
        );
    }

    #[test]
    fn test_to_str() {
        assert_eq!(time_string(0), "1970-01-01T00:00:00.000000");
        assert_eq!(time_string(1), "1970-01-01T00:00:00.000001");
        assert_eq!(time_string(1_000_001), "1970-01-01T00:00:01.000001");
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
        assert_eq!(0, FLOOR(999_999, 1));
        assert_eq!(1_000_000, FLOOR(1_000_000, 1));
        assert_eq!(1_000_000, FLOOR(1_000_111, 1));
        assert_eq!(10_000_000, FLOOR(10_123_111, 10));
        assert_eq!(10_000_000, FLOOR(19_123_111, 10));
        assert_eq!(20_000_000, FLOOR(29_123_111, 10));
    }

    #[test]
    fn test_ceil() {
        assert_eq!(1_000_000, CEIL(999_999, 1));
        assert_eq!(1_000_000, CEIL(1_000_000, 1));
        assert_eq!(2_000_000, CEIL(1_000_001, 1));
    }
}
