import unittest
from .candle import CandleExtended, time_as_nano
from datenano import datetime, MINUTE
from datetime import timedelta


class CandleTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.now = datetime.now().floor(MINUTE)
        self.candle = CandleExtended(self.now, MINUTE)

    def test_add_trades(self):
        time1 = self.now + timedelta(seconds=10)
        price1 = 1
        volume1 = 2

        self.candle.add_trade(time1, price1, volume1, False)
        self.assertEqual(self.candle.last_time, time_as_nano(time1))
        self.assertEqual(self.candle.open, price1)
        self.assertEqual(self.candle.close, price1)
        self.assertEqual(self.candle.high, price1)
        self.assertEqual(self.candle.low, price1)
        self.assertEqual(self.candle.volume, volume1)
        self.assertEqual(self.candle.trade_med, price1 * volume1)
        self.assertEqual(self.candle.trade_avg, price1 * volume1)
        self.assertEqual(self.candle.trade_cnt, 1)

        time2 = self.now + timedelta(seconds=20)
        price2 = 3
        volume2 = 2

        self.candle.add_trade(time2, price2, volume2, True)
        self.assertEqual(self.candle.last_time, time_as_nano(time2))
        self.assertEqual(self.candle.open, price1)
        self.assertEqual(self.candle.close, price2)
        self.assertEqual(self.candle.high, price2)
        self.assertEqual(self.candle.low, price1)
        self.assertEqual(self.candle.volume, volume2 + volume1)
        self.assertEqual(self.candle.trade_avg, ((price1 * volume1) + (price2 * volume2)) / 2)
        self.assertEqual(self.candle.trade_cnt, 2)


        time3 = self.now + timedelta(seconds=30)
        price3 = 2
        volume3 = 3

        self.candle.add_trade(time3, price3, volume3, True)
        self.assertEqual(self.candle.last_time, time_as_nano(time3))
        self.assertEqual(self.candle.open, price1)
        self.assertEqual(self.candle.close, price3)
        self.assertEqual(self.candle.high, price2)
        self.assertEqual(self.candle.low, price1)
        self.assertEqual(self.candle.volume, volume2 + volume1 + volume3)
        self.assertEqual(self.candle.trade_cnt, 3)


if __name__ == '__main__':
    unittest.main()
