use rust_decimal::{prelude::FromPrimitive, Decimal};
use super::format_number;
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
                "CLASS C"
            } else if monthly_profit < 10_000.0 {
                // class B
                "CLASS B"
            } else if monthly_profit < 100_000.0 {
                // class A
                "CLASS A"
            } else if monthly_profit < 1_000_000.0 {
                // class SS
                "CLASS S"
            } else {
                "CLASS SS"
            }
        }
        _ => "",
    };

    let monthly_profit = monthly_profit.floor() as i64;

        return format!(
            "PROFIT: {:>6}[{}]/({}[min]={:.2}[Month]) => {:>}[{}]/Month [{}]",
            format_number(profit as i64),
            config.home_currency,
            format_number(duration_min),
            duration_min as f64 / 30.0 / 24.0 / 60.0,
            format_number(monthly_profit),
            config.home_currency,
            class_string
        );
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
