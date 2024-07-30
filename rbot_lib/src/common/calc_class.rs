use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};

use super::{format_number, MarketConfig};

pub fn calc_class(config: &MarketConfig, profit: f64, duration_min: i64) -> String {
    if duration_min < 1 {
        return "".to_string();
    }

    let monthly_profit = profit * 30.0 * 24.0 * 60.0 / duration_min as f64;

    let profit: i64 = profit.floor() as i64;
    let monthly_profit: i64 = monthly_profit.floor() as i64;

    let class_string = match config.home_currency.to_uppercase().as_str() {
        "USD" | "USDT" | "USDC" => {
            if monthly_profit < -1_000_000 {
                // -ss
                "class -ss"
            } else {
                match monthly_profit {
                    -1_000_000..-100_000 => "class -s",
                    -100_000..-10_000 => "class -a",
                    -10_000..-1_000 => "class -b",
                    -1_000..-100 => "class -c",
                    -100..0 => "class -",
                    0..100 => "class +",
                    100..1_000 => "CLASS C",
                    1_000..10_000 => "CLASS B",
                    10_000..1_000_000 => "CLASS A",
                    _ => {
                        if 1_000_000 < monthly_profit {
                            "CLASS SS"
                        } else {
                            println!("unknown class {}", monthly_profit);
                            ""
                        }
                    }
                }
            }
        }
        _ => {""}
    };



    return format!(
        "PROFIT: {:6}[{}]/({}[min]={:.2}[Month]) => {:6}[{}]/Month [{}]",
        format_number(profit),
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
