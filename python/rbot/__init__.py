from .rbot import *
import signal

if hasattr(rbot, "__all__"):
    __all__ = rbot.__all__


def terminate(_a, _b):
    print("terminate", _a, _b)
    exit(0)
    
signal.signal(signal.SIGINT, terminate)

print("rbot version: ", rbot.__version__)
print("!!! ABSOLUTELY NO WARRANTY !!!")
print("!!!  USE AT YOUR OWN RISK  !!!")
print("See document at https://github.com/yasstake/rusty-bot")
print(" All rights reserved. (c) 2022-2023 rbot developers / yasstake")
