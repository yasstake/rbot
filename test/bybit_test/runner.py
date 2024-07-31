from rbot import Runner, BybitConfig, Bybit

class Agent:
    def on_init(self):
        print("call")
        pass

agent = Agent()


runner = Runner()

bybit = Bybit(True)
market = bybit.open_market(BybitConfig.BTCUSDT)

#runner.runrun()
runner.runrun(bybit, market, agent)
