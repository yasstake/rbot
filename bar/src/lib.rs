use std::io::{self, Write};

use indicatif::TermLike;

#[derive(Debug)]
struct CustomWriter {}

impl CustomWriter {
    fn new() -> Self {
        Self {}
    }

    fn write(&self, buf: &[u8]) -> io::Result<usize> {
        let mut stdout = io::stdout();
        stdout.write(buf)
    }

    fn flush(&self) -> io::Result<()> {
        io::stdout().flush()
    }
}

impl TermLike for CustomWriter {
    fn width(&self) -> u16 {
        120
    }

    fn move_cursor_up(&self, n: usize) -> io::Result<()> {
        let s = format!("\x1B[{}A", n);
        self.write(s.as_bytes())?;

        Ok(())
    }

    fn move_cursor_down(&self, n: usize) -> io::Result<()> {
        let s = format!("\x1B[{}B", n);
        self.write(s.as_bytes())?;

        Ok(())
    }

    fn move_cursor_right(&self, n: usize) -> io::Result<()> {
        let s = format!("\x1B[{}C", n);
        self.write(s.as_bytes())?;

        Ok(())
    }

    fn move_cursor_left(&self, n: usize) -> io::Result<()> {
        let s = format!("\x1B[{}D", n);
        self.write(s.as_bytes())?;

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
        self.write("\n".as_bytes())?;
        self.write("\x1B[K".as_bytes())?;

        Ok(())
    }

    fn flush(&self) -> io::Result<()> {
        self.flush()
    }
}



struct FileProgress{

}




#[cfg(test)]
mod tests {

    use std::{thread, time::Duration};

    use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
    use kdam::{term, tqdm, Bar, BarExt as _, RowManager};

    use super::*;

    #[test]
    fn test_bar() -> anyhow::Result<()> {
        // カスタムファイル出力先を開く
        let writer = CustomWriter::new();

        let target = ProgressDrawTarget::term_like(Box::new(writer));

        // プログレスバーの作成とカスタム出力先の設定
        let pb = ProgressBar::new(100);
        pb.set_draw_target(target);
        pb.set_style(ProgressStyle::default_bar().template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}",
        )?)
        ;

        //pb.set_draw_target(ProgressDrawTarget::stdout());

        for i in 0..100 {
            pb.inc(1);
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        pb.finish_with_message("done");

        Ok(())
    }



    #[test]
    fn test_kdam() {
        let total = 100;
        let mut pb = tqdm!(
            total = total,
            desc = "進捗状況",
            unit = "項目",
            unit_scale = true,
            dynamic_ncols = true,
            position = 1,
            leave = true
        );
    
        for i in 0..total {
            // 現在の処理内容を更新
            let current_task = format!("タスク {}/{} を処理中", i + 1, total);
            
            // 状況説明を表示（1行目）
            // eprintln!("{}", current_task);
            
            // プログレスバーを更新（2行目）
            pb.set_description(format!("進捗状況 ({}/{})", i + 1, total));
            pb.update(1);
    
            // 処理をシミュレート
            thread::sleep(Duration::from_millis(50));
        }
    
        eprintln!("\n処理完了");        
    }

    #[test]
    fn test_kdm2 () {
        let mut pb = tqdm!(total = 10);

        for i in 0..10 {
            std::thread::sleep(std::time::Duration::from_secs_f32(0.1));
    
            pb.update(1);
            pb.write(format!("Done task {}", i));
        }
    
        eprintln!();


    }

    #[test]
    fn test_multi_bar() {
        term::init(false);
        let _ = term::hide_cursor();
    
        for _ in tqdm!(0..4, desc = "1st loop", position = 0) {
            for _ in tqdm!(0..5, desc = "2nd loop", position = 1) {
                for _ in tqdm!(0..50, desc = "3rd loop", position = 2) {
                    std::thread::sleep(std::time::Duration::from_secs_f32(0.01));
                }
            }
        }
    
        eprint!("{}", "\n".repeat(3));
        println!("completed!");
    }

    #[test]
    fn test_without_macro() {
        let mut manager = RowManager::new(3);

        for (i, total) in [150, 100, 200, 400, 500, 600].iter().enumerate() {
            manager.push(tqdm!(
                total = *total,
                desc = format!("BAR {}", i),
                force_refresh = true
            ));
        }        
    }

    #[test]
    fn test_download() {
        let mut message_bar = Bar::new(100);
        message_bar.set_bar_format("{desc suffix=' '}{animation}{percentage}").unwrap();
        message_bar.set_description("------");

        let mut total_bar = Bar::new(100);
        total_bar.set_bar_format("{animation} {count}/{total} [{percentage:.0}%] in {elapsed human=true} ({rate:.1}/s)").unwrap();
         
        let mut sub_bar = Bar::new(100);

        let mut row = RowManager::new(3);
        row.push(message_bar);
        row.push(total_bar);
        row.push(sub_bar);

        for i in 0..100 {
            std::thread::sleep(std::time::Duration::from_secs_f32(0.1));

            row.get_mut(0).unwrap().set_description(format!("description {}", i));
            row.get_mut(0).unwrap().update_to(0);
            row.notify(0);

            row.get_mut(1).unwrap().update_to(i).unwrap();
            row.notify(1);

            row.get_mut(2).unwrap().update_to(i).unwrap();
            row.notify(2);
        }
    }

}
