import unittest
import math
from candle import CandleExtended
from datenano import datetime, MINUTE
from datetime import timedelta
from chart import Chart


class ChartTest(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.now = datetime.now().floor(MINUTE)
        self.chart = Chart(period=MINUTE)

    def test_add_trades(self):
        time1 = self.now + timedelta(seconds=10)
        price1 = 1
        volume1 = 2

        self.chart.add_trade(time1, price1, volume1, False)
        time2 = self.now + timedelta(seconds=20)
        price2 = 3
        volume2 = 2

        self.chart.add_trade(time2, price2, volume2, True)

        time3 = self.now + timedelta(seconds=30)
        price3 = 2
        volume3 = 3

        self.chart.add_trade(time3, price3, volume3, True)

        time4 = self.now + timedelta(minutes=2)
        price4 = 10
        volume4 = 5

        self.chart.add_trade(time4, price4, volume4, True)

        df = self.chart.dataframe

        self.assertEqual(df.iloc[0]["Volume"], volume1 + volume2 + volume3)
        self.assertEqual(df.iloc[0]["Open"], price1)
        self.assertEqual(df.iloc[0]["High"], price2)
        self.assertEqual(df.iloc[0]["Low"], price1)
        self.assertEqual(df.iloc[0]["Close"], price3)


if __name__ == '__main__':
    unittest.main()
