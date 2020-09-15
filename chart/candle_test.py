import unittest
from chart.candle import CandleExtended
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
        self.assertEqual(self.candle.lastTime, CandleExtended.time_nano(time1))
        self.assertEqual(self.candle.Open, price1)
        self.assertEqual(self.candle.Close, price1)
        self.assertEqual(self.candle.High, price1)
        self.assertEqual(self.candle.Low, price1)
        self.assertEqual(self.candle.Volume, volume1)
        self.assertEqual(self.candle.TradeMedian, price1 * volume1)
        self.assertEqual(self.candle.TradeAverage, price1 * volume1)
        self.assertEqual(self.candle.TradeCount, 1)

        time2 = self.now + timedelta(seconds=20)
        price2 = 3
        volume2 = 2

        self.candle.add_trade(time2, price2, volume2, True)
        self.assertEqual(self.candle.lastTime, CandleExtended.time_nano(time2))
        self.assertEqual(self.candle.Open, price1)
        self.assertEqual(self.candle.Close, price2)
        self.assertEqual(self.candle.High, price2)
        self.assertEqual(self.candle.Low, price1)
        self.assertEqual(self.candle.Volume, volume2 + volume1)
        self.assertEqual(self.candle.TradeAverage, ((price1 * volume1) + (price2 * volume2)) / 2)
        self.assertEqual(self.candle.TradeCount, 2)


        time3 = self.now + timedelta(seconds=30)
        price3 = 2
        volume3 = 3

        self.candle.add_trade(time3, price3, volume3, True)
        self.assertEqual(self.candle.lastTime, CandleExtended.time_nano(time3))
        self.assertEqual(self.candle.Open, price1)
        self.assertEqual(self.candle.Close, price3)
        self.assertEqual(self.candle.High, price2)
        self.assertEqual(self.candle.Low, price1)
        self.assertEqual(self.candle.Volume, volume2 + volume1 + volume3)
        self.assertEqual(self.candle.TradeCount, 3)


if __name__ == '__main__':
    unittest.main()
