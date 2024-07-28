use std::{
    borrow::BorrowMut,
    io::{self, Write as _},
};

use indicatif::TermLike;
use pyo3::{
    types::{IntoPyDict as _, PyAnyMethods as _},
    Bound, Py, PyAny, Python,
};

use super::{calc_class, MarketConfig};

pub struct FileBar {
    current_file: i64,
    current_file_size: i64,
    total_bar: Py<PyAny>,
    file_bar: Py<PyAny>,
}

impl FileBar {
    pub fn new(total_files: i64) -> Self {
        Python::with_gil(|py| {
            let tqdm = py.import_bound("tqdm").unwrap();
            let total_size = total_files * 100; // inpercent

            let kwargs = [("total", total_size)].into_py_dict_bound(py);
            let total_bar = tqdm.call_method("tqdm", (), Some(&kwargs)).unwrap();

            //            total_bar.setattr("bar_format",
            //                            "[{elapsed}] {n_fmt}/{total_fmt} {percentage.0f}% |{bar} |[ETA:{remaining}]");
            total_bar.setattr(
                "bar_format",
                "[{elapsed}]{percentage:>2.0f}% |{bar}| [ETA:{remaining}]",
            );

            let kwargs = [("total", total_size)].into_py_dict_bound(py);

            let file_bar = tqdm.call_method("tqdm", (), Some(&kwargs)).unwrap();

            file_bar.setattr(
                "bar_format",
                "{postfix:>30} ({percentage:2.0f}%) {n_fmt:>8}/{total_fmt} ({rate_fmt})",
            );

            file_bar.setattr("unit", "B");

            file_bar.setattr("unit_scale", true);

            Self {
                current_file: 0,
                current_file_size: 0,
                total_bar: total_bar.as_gil_ref().into(),
                file_bar: file_bar.as_gil_ref().into(),
            }
        })
    }

    pub fn set_total_files(&mut self, total_files: i64) {
        let total_bar = self.total_bar.borrow_mut();
        Python::with_gil(|py| {
            total_bar.setattr(py, "total", total_files * 100);
        });
        self.refresh();
    }

    pub fn set_filesize(&mut self, size: i64) {
        self.current_file_size = size;
        let file_bar = self.file_bar.borrow_mut();

        Python::with_gil(|py| {
            file_bar.setattr(py, "total", size);
        });

        self.refresh();
    }

    pub fn new_file(&mut self, name: &str, size: i64) {
        self.current_file += 1;
        self.current_file_size = size;

        let file_bar = self.file_bar.borrow_mut();

        Python::with_gil(|py| {
            file_bar.call_method1(py, "set_postfix_str", (name,));
            file_bar.setattr(py, "total", size);
            file_bar.setattr(py, "n", 0);
            file_bar.call_method0(py, "reset");
        });
        self.refresh();
    }

    pub fn file_pos(&mut self, n: i64) {
        let total_bar = self.total_bar.borrow_mut();
        let file_bar = self.file_bar.borrow_mut();

        Python::with_gil(|py| {
            total_bar.setattr(
                py,
                "n",
                (self.current_file - 1) * 100 + (n * 100) / self.current_file_size,
            );
            file_bar.setattr(py, "n", n);
        });

        self.refresh();
    }

    pub fn done() {}

    pub fn write_message(&mut self, message: &str) {
        let bar = self.total_bar.borrow_mut();
        Python::with_gil(|py| {
            bar.call_method1(py, "write", (message,));
        });
    }

    pub fn refresh(&mut self) {
        let total_bar = self.total_bar.borrow_mut();
        let file_bar = self.file_bar.borrow_mut();

        Python::with_gil(|py| {
            total_bar.call_method0(py, "refresh");
            file_bar.call_method0(py, "refresh");
        });

    }
}

pub struct RunningBar {
    profit: Py<PyAny>,
    progress: Py<PyAny>,
    message: Py<PyAny>,
}

impl RunningBar {
    pub fn new(duration: i64) -> Self {
        Python::with_gil(|py| {
            let tqdm = py.import_bound("tqdm.notebook").unwrap();
            let kwargs = [
                ("total", duration),
                ("position", 0),
                ("ncols", 100)
            ].into_py_dict_bound(py);
            let progress = tqdm.call_method("tqdm", (), Some(&kwargs)).unwrap();

            progress.setattr(
                "bar_format",
                "[{elapsed}]{percentage:>2.0f}% |{bar}| [ETA:{remaining}]",
            );

            let kwargs = [
                ("total", duration),
                ("position", 1)
            ].into_py_dict_bound(py);

            let profit = tqdm.call_method("tqdm", (), Some(&kwargs)).unwrap();

            profit.setattr("bar_format",
            "{postfix:>}",);

            let kwargs = [
                ("total", duration),
                ("position", 2)
            ].into_py_dict_bound(py);
            let message = tqdm.call_method("tqdm", (), Some(&kwargs)).unwrap();

            message.setattr("bar_format",
            "{postfix:<}",);


            Self {
                progress: progress.as_gil_ref().into(),
                profit: profit.as_gil_ref().into(),
                message: message.as_gil_ref().into(),
            }
        })
    }

    pub fn set_duration(&mut self, duration: i64) {
        let bar = self.progress.borrow_mut();
        Python::with_gil(|py| {
            bar.setattr(py, "total", duration);
        });
        self.refresh();
    }

    pub fn elapsed(&mut self, n: i64) {
        let progress = self.progress.borrow_mut();

        Python::with_gil(|py| {
            progress.setattr(py, "n", n);
            progress.call_method0(py, "refresh");
        });
        self.refresh();
    }

    pub fn done() {}

    pub fn set_profit(&mut self, config: &MarketConfig, profit: f64, duration_min: i64) {
        let profit_string = calc_class(config, profit, duration_min);

        let profit = self.profit.borrow_mut();

        Python::with_gil(|py| {
            profit.call_method1(py, "set_postfix_str", (&profit_string,));
        });

        self.refresh();
    }

    pub fn set_message(&mut self, msg: &str) {
        let message = self.message.borrow_mut();

        Python::with_gil(|py| {
            message.call_method1(py, "set_postfix_str", (msg,));
        });

        self.refresh();
    }

    pub fn print(&mut self, message: &str) {
        let bar = self.progress.borrow_mut();
        Python::with_gil(|py| {
            let kwargs = [("end", "\n")].into_py_dict_bound(py);

            bar.call_method_bound(py, "write", (message,), Some(&kwargs));
        });
        self.refresh();
    }

    pub fn refresh(&mut self) {
        let profit = self.profit.borrow_mut();
        let progress = self.progress.borrow_mut();
        let message = self.message.borrow_mut();

        Python::with_gil(|py| {
            profit.call_method0(py, "refresh");
            progress.call_method0(py, "refresh");
            message.call_method0(py, "refresh");
        });
    }
}

#[cfg(test)]
mod test_bar {
    use std::thread;

    use super::RunningBar;


    #[test]
    fn test_running_bar() {
        let mut bar = RunningBar::new(1000);

        for i in 0..1000 {
            bar.elapsed(i);
            thread::sleep(
                std::time::Duration::from_millis(10), // 100ミリ秒待機
            )
        }
    }
}
