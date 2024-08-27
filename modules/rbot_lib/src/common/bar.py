

class RestBar:
    def __init__(self, total_duration):
        self.progress = tqdm(total=total_duration, position=1, delay=1,
                             bar_format="[{elapsed}] ({percentage:>3.0f}%) |{bar}| [ETA:{remaining}]")
        self.status = tqdm(total=0, position=2, bar_format="{postfix:<}")

    def set_status(self, message):
        self.status.set_postfix_str(message)
    
    def diff_update(self, diff):
            self.progress.update(diff)

    def print(self, value):
        self.progress.write(value)

    def close(self):
        self.progress.close()
        self.file_progress.close()


class FileBar:
    def __init__(self, total_files):
        self.progress = tqdm(total=total_files * 100, position=1, delay=1,
                             bar_format="[{elapsed}] ({percentage:>3.0f}%) |{bar}| [ETA:{remaining}]")
        self.current_file = -1
        self.total_files = total_files
        self.last_progress = 0

        self.file_progress = tqdm(total=0, position=2, delay=1, bar_format="{postfix:7>} ({percentage:3.0f}%) |{bar}| {n_fmt:>8}/{total_fmt} {rate_fmt}",
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
            f"[{self.current_file + 1} / {self.total_files}]"
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



class ProgressBar:
    def __init__(self, max_value, has_bar):
        self.has_bar = has_bar
        if has_bar:
            self.progress = tqdm(total=max_value, position=1,
                                 bar_format="[{elapsed}]({percentage:>2.0f}%) |{bar}| [ETA:{remaining}]")

        self.last_progress = 0
        self.status = tqdm(total=0, position=2, bar_format="{postfix:<}")
        self.order = tqdm(total=0, position=3, bar_format="{postfix:<}")
        self.profit = tqdm(total=0, position=4, bar_format="{postfix:>}")

    def set_progress(self, value):
        if self.has_bar:
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
        if self.has_bar:
            self.progress.close()
        self.status.close()
        self.profit.close()
