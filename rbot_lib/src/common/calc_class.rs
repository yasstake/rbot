use indicatif::HumanCount;
use rust_decimal::{prelude::FromPrimitive, Decimal};

use super::MarketConfig;

pub fn calc_class(config: &MarketConfig, profit: f64, duration_min: i64) -> String {
    if duration_min < 1 {
        return "".to_string();
    }

    let monthly_profit = profit * 30.0 * 24.0 * 60.0 / duration_min as f64;

    let class_string = match config.home_currency.to_uppercase().as_str() {
        "USD" | "USDT" | "USDC" => {
            if monthly_profit < -1_000_000.0 {
                // -ss
                "class -ss"
            } else if monthly_profit < -100_000.0 {
                // -s
                "class -s"
            } else if monthly_profit < -10_000.0 {
                // -a
                "class -a"
            } else if monthly_profit < -1_000.0 {
                // -b
                "class -b"
            } else if monthly_profit < -100.0 {
                // -c
                "class -c"
            } else if monthly_profit < 0.0 {
                // -
                "class -"
            } else if monthly_profit < 100.0 {
                // +
                "class +"
            } else if monthly_profit < 1_000.0 {
                // class C
                "class C"
            } else if monthly_profit < 10_000.0 {
                // class B
                "class B"
            } else if monthly_profit < 100_000.0 {
                // class A
                "class A"
            } else if monthly_profit < 1_000_000.0 {
                // class SS
                "class S"
            } else {
                "class SS"
            }
        }
        _ => "",
    };

    let monthly_profit = Decimal::from_f64(monthly_profit).unwrap();
    let monthly_profit = config.round_price(monthly_profit);

    if monthly_profit.is_ok() {
        return format!(
            "PROFIT: {:6.2}[{}]/({}[min]={:.2}[Month]) => {:.2}[{}]/Month [{}]",
            profit,
            config.home_currency,
            duration_min,
            duration_min as f64 / 30.0 / 24.0 / 60.0,
            monthly_profit.unwrap(),
            config.home_currency,
            class_string
        );
    } else {
        return "****".to_string();
    }
}

#[cfg(test)]
mod class_calc_test {

    use crate::common::{calc_class, init_debug_log, MarketConfig};

    #[test]
    fn test_class() {
        let mut config = MarketConfig::default();
        config.home_currency = "USDT".to_string();

        init_debug_log();

        log::debug!("{}", calc_class(&config, -0.1, 1));
        log::debug!("{}", calc_class(&config, 0.1, 1));
        log::debug!("{}", calc_class(&config, -1.0, 1));
        log::debug!("{}", calc_class(&config, 1.0, 1));
        log::debug!("{}", calc_class(&config, -10.0, 1));
        log::debug!("{}", calc_class(&config, 10.0, 1));
        log::debug!("{}", calc_class(&config, -100.0, 1));
        log::debug!("{}", calc_class(&config, 100.0, 1));
    }
}
