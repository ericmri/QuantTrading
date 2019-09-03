# -*- coding: utf-8 -*-


# @author: msi
# 从tushare中加载5min。10min，日线，周线等各级别的实时数据并进行模拟测试
# LiveFeed参数：
#         :param identifiers: codes
#         :param ktype: 同tushare一样，ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
#         :param preload_start:若需要豫加载前面的数据，则设置开始时间，同tushare的start，str类型
#         :param apiCallDelay:后面每5分钟后调用ts的延时，一般用不到30秒
#         :param maxLen:

#
from pyalgotrade import strategy
from pyalgotrade.broker import backtesting
from pyalgotrade.technical import ma
# from pytdx.util import trade_date_sse
from cnx.tushare import TDXLiveFeed
import numpy as np
import talib



class LiveDemo_run(strategy.BaseStrategy):
    def __init__(self, feed, brk,instruments, n):
        strategy.BaseStrategy.__init__(self, feed, brk)
        self.__instruments = instruments
        self.__priceDataSeries = feed[self.__instruments[0]].getCloseDataSeries()
        self.__sma = ma.SMA(self.__priceDataSeries, n)
        self.__position = None

    def onEnterCanceled(self, position):
        self.__position = None

    def onEnterOK(self):
        pass

    def onExitOk(self, position):
        self.__position = None
        # self.info("long close")

    def onExitCanceled(self, position):
        self.__position.exitMarket()

    def onBars(self, bars):
        # If a position was not opened, check if we should enter a long position.
        for instrument in self.__instruments:
            if instrument in bars.getInstruments():
                try:
                    closeDs = self.getFeed().getDataSeries(instrument).getCloseDataSeries()
                    highDs = self.getFeed().getDataSeries(instrument).getHighDataSeries()
                    lowDs = self.getFeed().getDataSeries(instrument).getLowDataSeries()
                    volDs = self.getFeed().getDataSeries(instrument).getVolumeDataSeries()
                    vwp = np.array(closeDs)*np.array(volDs)
                    if len(closeDs)>55:
                        atr = talib.ATR(np.array(highDs),np.array(lowDs),np.array(closeDs),55)
                        vma = talib.MA(np.array(volDs), 55)
                        MA1 = talib.MA(vwp, 55)/vma
                        upband = MA1[-1]+atr[-1]
                    bar = bars[instrument]
                    print('running onbar', instrument,bar.getDateTime(), bar.getClose(),len(closeDs),upband)
                except:
                    print('running onbar error')
        # print ('running onbar',bar.getDateTime(), bar.getClose(), self.__sma[-1])
        print(self.getBroker().getPositions())

def testfun():
    liveFeed = TDXLiveFeed.LiveFeed(['300653','002117','000001','600000','000043'], 'D', preload_start='2019-05-10')
    brk = backtesting.Broker(1000, liveFeed)
    brk.setShares('300653',200,62.525)
    brk.setShares('002117',1000,10.485)
    brk.setShares('000043',1000,12.003)
    strat = LiveDemo_run(liveFeed, brk, ['300653','002117','000001','600000','000043'], 55)
    strat.run()
# def testStrategytushare():
# #     strat = LiveDemo_run
# #     liveFeed = tushareLiveFeed.LiveFeed(['600848'], '5', preload_start='2017-01-01')
# #     brk = backtesting.Broker(1000, liveFeed)
# #     strat = LiveDemo_run(liveFeed, brk, ['600848'], 3)
# #     strat.run()
#
#
if __name__ == "__main__":
    testfun()
# liveFeed = TDXLiveFeed.LiveFeed(['300653','002117','000001','600000','000043'], 'D', preload_start='2019-05-10')
# brk = backtesting.Broker(1000, liveFeed)
# brk.setShares('300653',200,62.525)
# brk.setShares('002117',1000,10.485)
# brk.setShares('000043',1000,12.003)
# strat = LiveDemo_run(liveFeed, brk, ['300653','002117','000001','600000','000043'], 55)
# strat.run()























