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

const PY_BAR: &str = include_str!("./bar.py");



pub struct PyRestBar {
    bar: Py<PyAny>,
    enable: bool,
    verbose_print: bool,
}

impl PyRestBar {
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
            format!("{}{}", PY_TQDM_NOTEBOOK, PY_BAR)
        } else {
            format!("{}{}", PY_TQDM_PYTHON, PY_BAR)
        };

        Python::with_gil(|py| {
            let py_module =
                PyModule::from_code_bound(py, &py_script, "py_file_bar.py", "py_file_bar");

            if py_module.is_err() {
                log::error!("py_file_bar tqdm bar class create error")
            }

            let py_module = py_module.unwrap();

            let progress_class = py_module.getattr("RestBar").unwrap();
            let bar = progress_class.call1((total_duration,)).unwrap();

            self.bar = bar.into();
            self.enable = enable;
            self.verbose_print = verbose_print;
        });
    }

    pub fn diff_update(&mut self, diff: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            // ignore err.
            let _r = bar.call_method1(py, "diff_update", (diff,));
        })
    }

    pub fn set_status(&mut self, message: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            // ignore err.
            let _r = bar.call_method1(py, "set_status", (message,));
        })
    }

    pub fn print(&mut self, m: &str) {
        if ! self.verbose_print {
            return;
        }

        let bar = self.bar.borrow_mut();


        Python::with_gil(|py| {
            if self.enable {
                let _r = bar.call_method1(py, "print", (m,));
            }
            else {
                println!("{}", m);
            }
        });
    }
}





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
            format!("{}{}", PY_TQDM_NOTEBOOK, PY_BAR)
        } else {
            format!("{}{}", PY_TQDM_PYTHON, PY_BAR)
        };

        Python::with_gil(|py| {
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
            // ignore err.
            let _r = bar.call_method1(py, "set_file_progress", (n,));
        })
    }

    pub fn set_total_files(&mut self, total_files: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            let _r = bar.call_method1(py, "set_total_files", (total_files,));
        })
    }


    pub fn next_file(&mut self, file_name: &str, size: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            let _r = bar.call_method1(py, "next_file", (file_name, size,));
        })
    }

    pub fn set_file_size(&mut self, size: i64) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            let _r = bar.call_method1(py, "set_file_size", (size,));
        })
    }

    pub fn print(&mut self, m: &str) {
        if ! self.verbose_print {
            return;
        }

        let bar = self.bar.borrow_mut();


        Python::with_gil(|py| {
            if self.enable {
                let _r = bar.call_method1(py, "print", (m,));
            }
            else {
                println!("{}", m);
            }
        });
    }
}



const PY_RUNNING_BAR: &str = r#"

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

    pub fn init(&mut self, total_duration: i64, has_bar: bool, enable: bool, verbose_print: bool) {
        let py_script = if is_notebook() {
            format!("{}{}", PY_TQDM_NOTEBOOK, PY_BAR)
        } else {
            format!("{}{}", PY_TQDM_PYTHON, PY_BAR)
        };

        Python::with_gil(|py| {
            let py_module =
                PyModule::from_code_bound(py, &py_script, "py_running_bar.py", "py_running_bar");

            if py_module.is_err() {
                log::error!("py progress tqdm bar class create error")
            }

            let py_module = py_module.unwrap();

            let progress_class = py_module.getattr("ProgressBar").unwrap();
            let bar = progress_class.call1((total_duration, has_bar)).unwrap();

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
            let _r = bar.call_method1(py, "set_progress", (n,));
        })
    }

    pub fn message(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            let _r = bar.call_method1(py, "print_message", (m,));
        })
    }

    pub fn order(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            let _r = bar.call_method1(py, "print_order", (m,));
        })
    }

    pub fn profit(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        Python::with_gil(|py| {
            let _r = bar.call_method1(py, "print_profit", (m,));
        })
    }


    pub fn print(&mut self, m: &str) {
        if ! self.enable {
            return;
        }
        let bar = self.bar.borrow_mut();

        if self.verbose_print {
            Python::with_gil(|py| {
                let _r = bar.call_method1(py, "print", (m,));
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

    use crate::common::DAYS;

    use super::{PyFileBar, PyRestBar, PyRunningBar};

    #[test]
    fn test_py_restbar() {
        let mut bar = PyRestBar::new();
        bar.init(DAYS(1), true, true);

        for i in 0..10000 {
            bar.diff_update(5000000);
            thread::sleep(std::time::Duration::from_millis(5));
            bar.set_status(&format!("count {}", i));
        }
    }


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
        bar.init(1000, true, true, true);

        bar.set_progress(100);
        bar.message("NOW IN PROGRESS");
        bar.profit("SO MUCH PROFIT");
        bar.print("log message");
        bar.order("MYORDER");
        bar.message("NOW IN PROGRESS2");
        bar.profit("SO MUCH PROFIT2");
        bar.set_progress(200);
    }


    #[test]
    fn test_init_no_bar() {
        let mut bar = PyRunningBar::new();
        bar.init(1000, false, true, true);

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
