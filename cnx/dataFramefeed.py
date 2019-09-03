# -*- coding: utf-8 -*-
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
.. 
u用以转换dataFrame到feed，相当于以pandas dataframe 为桥，不再以csv为桥。下一步增加方法直接从数据库中读
"""

import datetime

from pyalgotrade.barfeed import common
from pyalgotrade.utils import dt
from pyalgotrade import dataseries

import cnx.dataFrameBarfeed as dataFrameBarfeed
import cnx.bar as bar


######################################################################
## Yahoo Finance CSV parser
# Each bar must be on its own line and fields must be separated by comma (,).
#
# Bars Format:
# Date,Open,High,Low,Close,Volume,Adj Close
#
# The csv Date column must have the following format: YYYY-MM-DD

def parse_date(date):
    # Sample: 2005-12-30
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")
    year = int(date[0:4])
    month = int(date[5:7])
    day = int(date[8:10])
    ret = datetime.datetime(year, month, day)
    return ret
def parse_date16(date):
    # Sample: '%Y-%m-%d %H:%M'
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")
    year = int(date[0:4])
    month = int(date[5:7])
    day = int(date[8:10])
    hour = int(date[11:13])
    minute = int(date[14:16])
    ret = datetime.datetime(year, month, day,hour,minute)
    return ret
def parse_date19(date):
    # Sample: '%Y-%m-%d %H:%M:%S'
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")
    year = int(date[0:4])
    month = int(date[5:7])
    day = int(date[8:10])
    hour = int(date[11:13])
    minute = int(date[14:16])
    second = int(date[17:19])
    ret = datetime.datetime(year, month, day,hour,minute,second)
    return ret

def parse_date23(date):
    # Sample: '%Y-%m-%d %H:%M:%S.000'
    # This custom parsing works faster than:
    # datetime.datetime.strptime(date, "%Y-%m-%d")
    year = int(date[0:4])
    month = int(date[5:7])
    day = int(date[8:10])
    hour = int(date[11:13])
    minute = int(date[14:16])
    second = int(date[17:19])
    microsecond = int(date[20:23])*1000
    ret = datetime.datetime(year, month, day,hour,minute,second,microsecond)
    return ret


class RowParser(dataFrameBarfeed.RowParser):
    def __init__(self, dailyBarTime, frequency, timezone=None, sanitize=False):
        self.__dailyBarTime = dailyBarTime
        self.__frequency = frequency
        self.__timezone = timezone
        self.__sanitize = sanitize

    def __parseDate(self, dateString):
        ret = parse_date(dateString)
        # Time on Yahoo! Finance CSV files is empty. If told to set one, do it.
        if self.__dailyBarTime is not None:
            ret = datetime.datetime.combine(ret, self.__dailyBarTime)
        # Localize the datetime if a timezone was given.
        if self.__timezone:
            ret = dt.localize(ret, self.__timezone)
        return ret

    def getFieldNames(self):
        # It is expected for the first row to have the field names.
        return None

    def getDelimiter(self):
        return ","
    
    #对dataFrame的每行进行操作
    def handler(x):
        pass
    #row的结构 row[0]为时间，string类型。row[1]为Series类型:'open'\high\close\low\volume\amoun或price——change等，前面6项和tushare 对应  
    def parseBar(self, row):
        if isinstance(row[0],str) or isinstance(row[0],str) :
            if len(row[0].strip())==19:
                dateTime = parse_date19(row[0]) #date
            elif len(row[0].strip())==16:  #tushare～～～～16位数据
                dateTime = parse_date16(row[0])  # date
            else:
               dateTime = parse_date(row[0]) #date
        else:
            dateTime =row[0]
        close = float(row[1]['close'])
        open_ = float(row[1]['open'])
        high = float(row[1]['high'])
        low = float(row[1]['low'])
        volume = float(row[1]['vol'])
        adjClose = float(row[1]['close'])#后续需要实现
        
        if self.__sanitize:
            open_, high, low, close = common.sanitize_ohlc(open_, high, low, close)

        return bar.BasicBar(dateTime, open_, high, low, close, volume, adjClose, self.__frequency)

        # row的结构 row[0]为时间，string类型。row[1]为Series类型:'open'\high\close\low\volume\amoun或price——change等，前面6项和tushare 对应

    def parseTickBar(self,id,row):
        """
        转换tick格式的bar,将‘ap’或‘ap1’作为tickds.__apDataSeries 以及bar.__ap
        :param row:,tick的格式稍微简单，设置一个defaluttick format
        :return:
        """
        tmp_extra = {}
        if isinstance(id,str) or isinstance(id,str):
            id = parse_date23(id)

        for key in row.index:
                # extract extra component
            if key not in ['open','ap1','bp1','av1','bv1','high', 'low', 'close', 'volume', 'amount', 'preclose'
            , 'new_price', 'bought_amount', 'sold_amount', 'bought_volume', 'sold_volume'
            , 'frequency']:
                tmp_extra[key] = row[key]

        return bar.BasicTick(id, float(row['open']), float(row['high']), float(row['low']), float(row['close']), float(row['volume'])
                                   , float(row['amount']), float(row['bp1']),float(row['bv1']),float(row['ap1']), float(row['av1']), float(row['preclose']),
                                   float(row['bought_volume'])
                                   , float(row['sold_volume']), float(row['bought_amount']), float(row['sold_amount']),
                             bar.Frequency.TRADE, False,
                                   tmp_extra)

class Feed(dataFrameBarfeed.BarFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Yahoo! Finance.

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the opposite end.
    :type maxLen: int.

    .. note::
        Yahoo! Finance csv files lack timezone information.
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.DAY, timezone=None, maxLen=dataseries.DEFAULT_MAX_LEN):
        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if frequency not in [bar.Frequency.DAY, bar.Frequency.WEEK, bar.Frequency.MINUTE]:
            raise Exception("Invalid frequency.")

        dataFrameBarfeed.BarFeed.__init__(self, frequency, maxLen)
        self.__timezone = timezone
        self.__sanitizeBars = False

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize

    def barsHaveAdjClose(self):
        return True

    def addBarsFromDataFrame(self, instrument,dataFrame,timezone=None):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """

        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None:
            timezone = self.__timezone

        rowParser = RowParser(self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars)
        dataFrameBarfeed.BarFeed.addBarsFromDataFrame(self, instrument,rowParser,dataFrame)

class TickFeed(dataFrameBarfeed.TickFeed):
    """A :class:`pyalgotrade.barfeed.csvfeed.BarFeed` that loads bars from CSV files downloaded from Yahoo! Finance.

    :param frequency: The frequency of the bars. Only **pyalgotrade.bar.Frequency.DAY** or **pyalgotrade.bar.Frequency.WEEK**
        are supported.
    :param timezone: The default timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
    :type timezone: A pytz timezone.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded from the opposite end.
    :type maxLen: int.

    .. note::
        Yahoo! Finance csv files lack timezone information.
        When working with multiple instruments:

            * If all the instruments loaded are in the same timezone, then the timezone parameter may not be specified.
            * If any of the instruments loaded are in different timezones, then the timezone parameter must be set.
    """

    def __init__(self, frequency=bar.Frequency.TRADE, timezone=None,maxLen=dataseries.DEFAULT_MAX_LEN):
        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")
        dataFrameBarfeed.TickFeed.__init__(self, frequency, maxLen)
        self.__timezone = timezone
        self.__sanitizeBars = False
        self.__datetime_format = '%Y-%m-%d %H:%M:%S.%f'

    def sanitizeBars(self, sanitize):
        self.__sanitizeBars = sanitize
    def set_datetime_format(self,datetime_format):
        self.__datetime_format = datetime_format
    def barsHaveAdjClose(self):
        return True

    def addBarsFromDataFrame(self, instrument,dataFrame,timezone=None):
        """Loads bars for a given instrument from a CSV formatted file.
        The instrument gets registered in the bar feed.

        :param instrument: Instrument identifier.
        :type instrument: string.
        :param path: The path to the CSV file.
        :type path: string.
        :param timezone: The timezone to use to localize bars. Check :mod:`pyalgotrade.marketsession`.
        :type timezone: A pytz timezone.
        """

        if isinstance(timezone, int):
            raise Exception("timezone as an int parameter is not supported anymore. Please use a pytz timezone instead.")

        if timezone is None:
            timezone = self.__timezone
        dataFrame = dataFrame.sort_values(by='datetime')
        dataFrame.drop_duplicates('datetime', inplace=True)
        #dftypes = dataFrame['datetime'].values[0]
        #if isinstance(dftypes,str) or isinstance(dftypes,unicode) :
        #    dataFrame.index = dataFrame.index.apply(
       #         lambda x: datetime.datetime.strptime(x, self.__datetime_format))

        read_list = ['open', 'high', 'low', 'close', 'volume', 'amount', 'preclose'
            , 'new_price', 'bought_amount', 'sold_amount', 'bought_volume', 'sold_volume'
            , 'frequency']
        for add in read_list:
            if add not in dataFrame.columns:
                dataFrame[add] = 0
        rowParser = RowParser(self.getDailyBarTime(), self.getFrequency(), timezone, self.__sanitizeBars)
        dataFrameBarfeed.TickFeed.addBarsFromDataFrame(self, instrument,rowParser,dataFrame)