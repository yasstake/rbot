
from rbot import Bybit, BybitConfig
from rbot import Broadcast

broadcast = Broadcast("")

msg = broadcast.receive()

print(msg)


