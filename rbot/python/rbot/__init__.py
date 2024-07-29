from .rbot import *
import signal
import sys

if hasattr(rbot, "__all__"):
    __all__ = rbot.__all__


def terminate(_a, _b):
    print("terminate", _a, _b)
    exit(0)
    
signal.signal(signal.SIGINT, terminate)

print("rbot version: ", rbot.__version__)
print("!!! ABSOLUTELY NO WARRANTY. USE AT YOUR OWN RISK !!!")
print("For some exchange, an affliate or referer link may be included.")
print("Distributed under LGPL license. https://www.gnu.org/licenses/lgpl-3.0.txt")
print("See the document at https://github.com/yasstake/rusty-bot")
print("All rights reserved. (c) 2022-2024 rbot(rusty-bot) developers / yasstake")




def mount_google_drive(rbot_path = 'RUSTY-BOT'):
    if 'google.clab' in sys.modules:
        from google.colab import drive
        target = '/content/drive/' + rbot_path
        drive.mount(target)
        rbot.set_db_root(target)
        print('rbot db is created in [' + target + ']')
    else:
        print('NOT running on Google Colab')

