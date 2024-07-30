use std::{
    borrow::BorrowMut,
    io::{self, Write as _},
};

use pyo3::{
    types::{IntoPyDict as _, PyAnyMethods as _, PyModule},
    Bound, Py, PyAny, Python,
};

use super::{calc_class, is_notebook, MarketConfig};


const PY_TQDM_PYTHON: &str = r#"
from tqdm import tqdm
"#;

const PY_TQDM_NOTEBOOK: &str = r#"
from tqdm.notebook import tqdm
"#;

const PY_FILE_BAR: &str = r#"

class FileBar:
    def __init__(self, total_files):
        self.progress = tqdm(total=total_files * 100, position=1, delay=2,
                             bar_format="[{elapsed}]({percentage:>3.0f}%) |{bar}| [ETA:{remaining}]")
        self.current_file = -1
        self.total_files = total_files
        self.last_progress = 0

        self.file_progress = tqdm(total=0, position=2, delay=2, bar_format="       ({percentage:3.0f}%) |{bar}| {postfix:>} {n_fmt:>8}/{total_fmt} {rate_fmt}",
          unit="B", unit_scale=True
        )
        self.last_file_progress = 0
        self.current_file_size = 0

    def set_total_files(self, value):
        self.progress.reset(total=value * 100)
        self.current_file = -1
        self.total_files = value
        self.last_progress = 0
        self.last_file_progress = 0
        self.current_file_size = 0
        self.file_progress.reset(total=0)

    def set_file_size(self, size):
        self.current_file_size = size
        self.last_file_progress = 0
        self.file_progress.reset(total=size)

    def set_file_progress(self, value):
        diff = value - self.last_file_progress
        if diff > 0:
            self.file_progress.update(diff)

        self.last_file_progress = value

        if self.current_file <= 0:
            return

        self.set_progress(
            self.current_file * 100 +
            (value * 100 / self.current_file_size)
        )

    def set_progress(self, value):
        diff = value - self.last_progress
        if diff > 0:
            self.progress.update(diff)

        self.last_progress = value

    def next_file(self, name, size):
        self.current_file += 1
        self.set_progress(self.current_file * 100)

        self.last_file_progress = 0
        self.current_file_size = size
        self.file_progress.reset(total =size)
        self.file_progress.set_description(name)
        self.file_progress.set_postfix_str(
            f"[{self.current_file} / {self.total_files}]"
        )

    def set_progress(self, value):
        diff = value - self.last_progress
        if diff > 0:
            self.progress.update(diff)

        self.last_progress = value

    def print(self, value):
        self.progress.write(value)

    def close(self):
        self.progress.close()
        self.file_progress.close()
"#;

pub struct PyFileBar {
    bar: Py<PyAny>,
    enable: bool,
    verbose_print: bool,
}

impl PyFileBar {
    pub fn new() -> Self {
        let none = Python::with_gil(|py|{
            Python::None(py)
        });

        Self {
            bar: none,
            enable: false,
            verbose_print: false
        }
    }

    pub fn init(&mut self, total_files: i64, enable: bool, verbose_print: bool) {
        let py_script = if is_notebook() {
            format!("{}{}", PY_TQDM_NOTEBOOK, PY_FILE_BAR)
        } else {
            format!("{}{}", PY_TQDM_PYTHON, PY_FILE_BAR)
        };

        let bar = Python::with_gil(|py| {
            let py_module =
                PyModule::from_code_bound(py, &py_script, "py_file_bar.py", "py_file_bar");

            if py_module.is_err() {
                log::error!("py_file_bar tqdm bar class create error")
            }

            let py_module = py_module.unwrap();

            let progress_class = py_module.getattr("FileBar").unwrap();
            let bar = progress_class.call1((total_files,)).unwrap();

            self.bar = bar.into();
            self.enable = enable;
            self.verbose_print = verbose_print;
        });
    }

    pub fn set_file_progress(&mut self, n: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "set_file_progress", (n,)).unwrap();
        })
    }

    pub fn set_total_files(&mut self, total_files: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "set_total_files", (total_files,)).unwrap();
        })
    }


    pub fn next_file(&mut self, file_name: &str, size: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "next_file", (file_name, size,)).unwrap();
        })
    }

    pub fn set_file_size(&mut self, size: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "set_file_size", (size,)).unwrap();
        })
    }

    pub fn print(&mut self, m: &str) {
        if ! self.verbose_print {
            return;
        }

        let bar = self.bar.borrow_mut();


        Python::with_gil(|py| {
            if self.enable {
                bar.call_method1(py, "print", (m,)).unwrap();
            }
            else {
                println!("{}", m);
            }
        });
    }
}



const PY_RUNNING_BAR: &str = r#"

class ProgressBar:
    def __init__(self, max_value):
        self.progress = tqdm(total=max_value, position=1,
                             bar_format="[{elapsed}]({percentage:>2.0f}%) |{bar}| [ETA:{remaining}]")
        self.last_progress = 0
        self.status = tqdm(total=0, position=2, bar_format="{postfix:<}")
        self.order = tqdm(total=0, position=3, bar_format="{postfix:<}")
        self.profit = tqdm(total=0, position=4, bar_format="{postfix:>}")

    def set_progress(self, value):
        diff = value - self.last_progress
        if diff > 0:
            self.progress.update(diff)

        self.last_progress = value

    def print_message(self, value):
        self.status.set_postfix_str(value)

    def print_order(self, value):
        self.order.set_postfix_str(value)

    def print_profit(self, value):
        self.profit.set_postfix_str(value)

    def print(self, value):
        self.progress.write(value)

    def close(self):
        self.progress.close()
        self.status.close()
        self.profit.close()
"#;

pub struct PyRunningBar {
    bar: Py<PyAny>,
    enable: bool,
    verbose_print: bool,
}

impl PyRunningBar {
    pub fn new() -> Self {
        let none = Python::with_gil(|py|{
            Python::None(py)
        });

        Self {
            bar: none,
            enable: false,
            verbose_print: false
        }
    }

    pub fn init(&mut self, total_duration: i64, enable: bool, verbose_print: bool) {
        let py_script = if is_notebook() {
            format!("{}{}", PY_TQDM_NOTEBOOK, PY_RUNNING_BAR)
        } else {
            format!("{}{}", PY_TQDM_PYTHON, PY_RUNNING_BAR)
        };

        let bar = Python::with_gil(|py| {
            let py_module =
                PyModule::from_code_bound(py, &py_script, "py_running_bar.py", "py_running_bar");

            if py_module.is_err() {
                log::error!("py progress tqdm bar class create error")
            }

            let py_module = py_module.unwrap();

            let progress_class = py_module.getattr("ProgressBar").unwrap();
            let bar = progress_class.call1((total_duration,)).unwrap();

            self.bar = bar.into();
            self.enable = enable;
            self.verbose_print = verbose_print;
        });
    }

    pub fn set_progress(&mut self, n: i64) {
        if ! self.enable {
            return;
        }

        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "set_progress", (n,)).unwrap();
        })
    }

    pub fn message(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "print_message", (m,)).unwrap();
        })
    }

    pub fn order(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "print_order", (m,)).unwrap();
        })
    }

    pub fn profit(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            bar.call_method1(py, "print_profit", (m,)).unwrap();
        })
    }


    pub fn print(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        if self.verbose_print {
            Python::with_gil(|py| {
                bar.call_method1(py, "print", (m,)).unwrap();
            })
        }
        else {
            println!("{}", m);
        }
    }
}

#[cfg(test)]
mod test_bar {
    use std::thread;

    use super::{PyFileBar, PyRunningBar};


    #[test]
    fn test_py_filebar() {
        let mut bar = PyFileBar::new();
        bar.init(10, true, true);

        bar.set_total_files(11);

        for i in 0..10 {
            bar.next_file(&format!("test {}", i), 10_000);

            for j in 0..100 {
                bar.set_file_progress(j * 100);
                thread::sleep(
                    std::time::Duration::from_millis(10));
   
            }

            bar.print("--next--")
        }
    }

    #[test]
    fn test_init_bar() {
        let mut bar = PyRunningBar::new();
        bar.init(1000, true, true);

        bar.set_progress(100);
        bar.message("NOW IN PROGRESS");
        bar.profit("SO MUCH PROFIT");
        bar.print("log message");
        bar.order("MYORDER");
        bar.message("NOW IN PROGRESS2");
        bar.profit("SO MUCH PROFIT2");
        bar.set_progress(200);
    }
}
