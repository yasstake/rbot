use std::{
    borrow::BorrowMut,
    io::{self, Write as _},
};

use indicatif::TermLike;
use pyo3::{
    types::{IntoPyDict as _, PyAnyMethods as _},
    Bound, Py, PyAny, Python,
};

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
                "[{elapsed}]{percentage:2.0f}% |{bar}| [ETA:{remaining}]]",
            );

            let kwargs = [("total", total_size)].into_py_dict_bound(py);
            let file_bar = tqdm.call_method("tqdm", (), Some(&kwargs)).unwrap();

            file_bar.setattr(
                "bar_format",
                "[{postfix:>30}] ({percentage:2.0f}%)  [{n_fmt:>6}/{total_fmt:<6}] ({rate_fmt})",
            );

            Self {
                current_file: 0,
                current_file_size: 0,
                total_bar: total_bar.as_gil_ref().into(),
                file_bar: file_bar.as_gil_ref().into(),
            }
        })
    }

    pub fn new_file(&mut self, name: &str, size: i64) {
        self.current_file += 1;
        self.current_file_size = size;

        //        let bar = self.file_bar.borrow_mut();
        let file_bar = self.file_bar.borrow_mut();

        Python::with_gil(|py| {
            let kwargs = [("downloading", name)].into_py_dict_bound(py);

            //file_bar.call_method_bound(py, "set_postfix_str", (),Some(&kwargs));
            //file_bar.call_method_bound(py, "set_postfix", (name, ),None);
            file_bar.call_method1(py, "set_postfix_str", (name,));
            file_bar.setattr(py, "total", size);
            file_bar.setattr(py, "n", 0);
            file_bar.call_method0(py, "reset");
            file_bar.call_method0(py, "refresh");
        });
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

            total_bar.call_method0(py, "refresh");
            file_bar.call_method0(py, "refresh");
        });
    }

    pub fn file_done() {}
}

#[derive(Debug)]
pub struct PyWriter {}

impl PyWriter {
    pub fn new() -> Self {
        Self {}
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let msg = String::from_utf8(buf.to_vec()).unwrap();
        let msg = msg.as_str();

        self.write_raw_str(msg);

        Ok(buf.len())
    }

    fn write_raw_str(&self, msg: &str) {
        Python::with_gil(|py| {
            let sys = py.import_bound("sys").unwrap();
            let stderr = sys.getattr("stdout").unwrap();
            stderr.call_method("write", (msg,), None).unwrap();
            stderr.call_method0("flush").unwrap();
        });
    }

    fn flush(&self) -> io::Result<()> {
        Python::with_gil(|py| {
            let sys = py.import_bound("sys").unwrap();
            let stderr = sys.getattr("stdout").unwrap();
            stderr.call_method0("flush").unwrap();
        });

        Ok(())
    }
}

impl TermLike for PyWriter {
    fn width(&self) -> u16 {
        100
    }

    fn move_cursor_up(&self, n: usize) -> io::Result<()> {
        //let s = format!("\x1B[{}A", n);
        //self.write(s.as_bytes())?;
        self.write_raw_str(format!(r"\33[{}A", n).as_str());

        Ok(())
    }

    fn move_cursor_down(&self, n: usize) -> io::Result<()> {
        //let s = format!("\x1B[{}B", n);
        //self.write(s.as_bytes())?;
        self.write_raw_str(format!(r"\33[{}B", n).as_str());

        Ok(())
    }

    fn move_cursor_right(&self, n: usize) -> io::Result<()> {
        //let s = format!("\x1B[{}C", n);
        //self.write(s.as_bytes())?;
        self.write_raw_str(format!(r"\33[{}C", n).as_str());

        Ok(())
    }

    fn move_cursor_left(&self, n: usize) -> io::Result<()> {
        //let s = format!("\x1B[{}D", n);
        //self.write(s.as_bytes())?;
        self.write_raw_str(format!(r"\33[{}D", n).as_str());

        Ok(())
    }

    fn write_line(&self, s: &str) -> io::Result<()> {
        let r = self.write(s.as_bytes());
        self.write("\n".as_bytes())?;

        if r.is_err() {
            return Err(r.unwrap_err());
        }

        Ok(())
    }

    fn write_str(&self, s: &str) -> io::Result<()> {
        let r = self.write(s.as_bytes());

        if r.is_err() {
            return Err(r.unwrap_err());
        }

        Ok(())
    }

    fn clear_line(&self) -> io::Result<()> {
        self.write_raw_str(r"\r");
        self.write(r"\33[K".as_bytes())?;

        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {

    use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

    use super::*;

    #[test]
    fn test_bar() -> anyhow::Result<()> {
        // カスタムファイル出力先を開く
        let writer = PyWriter::new();

        let target = ProgressDrawTarget::term_like(Box::new(writer));

        // プログレスバーの作成とカスタム出力先の設定
        let pb = ProgressBar::new(100);
        pb.set_draw_target(target);
        pb.set_style(ProgressStyle::default_bar().template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}",
        )?);

        //pb.set_draw_target(ProgressDrawTarget::stdout());

        for i in 0..100 {
            pb.inc(1);
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        pb.finish_with_message("done");

        Ok(())
    }
}
