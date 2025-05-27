from .rbot import *
import signal
import sys

if hasattr(rbot, "__all__"): # type: ignore
    __all__ = rbot.__all__ # type: ignore


def terminate(_a, _b):
    print("terminate", _a, _b)
    exit(0)
    
signal.signal(signal.SIGINT, terminate)



def mount_google_drive(rbot_path = 'RUSTY-BOT'):
    import sys
    if 'google.colab' in sys.modules:
        from google.colab import drive
        target = '/content/drive/'
        drive.mount(target)
        drive_path = target + 'MyDrive' + '/' +  rbot_path
        rbot.set_db_root(drive_path)
        print('rbot db path is  [' + drive_path  + ']')
    else:
        print('NOT running on Google Colab')


print("rbot version: ", rbot.__version__)
print("!!! ABSOLUTELY NO WARRANTY. USE AT YOUR OWN RISK !!!")
print("For some exchange, an affliate or referer link may be included.")
print("Distributed under LGPL license. https://www.gnu.org/licenses/lgpl-3.0.txt")
print("See the document at https://github.com/yasstake/rusty-bot")
print("All rights reserved. (c) 2022-2025 rbot(rusty-bot) developers / yasstake")

