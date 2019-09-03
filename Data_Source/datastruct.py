# -*- coding: utf-8 -*-

from datetime import timedelta
import logging

logger = logging.Logger('data')


class QError(Exception):
    msg = None

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.message = str(self)

    def __str__(self):
        msg = self.msg.format(**self.kwargs)
        return msg

    __unicode__ = __str__
    __repr__ = __str__

class PeriodTypeError(QError):
    msg = "不存在该周期！ -- {period}"

class Contract(object):
    """ 合约。

    :ivar exchange: 市场类型。
    :ivar code: 合约代码
    :ivar is_stock: 是否是股票
    :ivar margin_ratio: 保证金比例。
    :ivar volume_multiple: 合约乘数。
    """
    info = None
    source_type = None

    def __init__(self, str_contract):
        ## @TODO 修改参数为（code, exchange)
        info = str_contract.split('.')
        if len(info) == 2:
            code = info[0].upper()
            exchange = info[1].upper()
        else:
            logger.error('错误的合约格式: %s' % str_contract)
            logger.exception()
        self.exchange = exchange
        self.code = code
        if self.exchange == 'SZ' or self.exchange == 'SH':
            self.is_stock = True
        elif self.exchange == 'SHFE':
            self.is_stock = False
        elif self.exchange == 'IDX':
            self.is_stock = False
        elif self.exchange == 'TEST' and self.code == 'STOCK':
            self.is_stock = True
        elif self.exchange == 'TEST':
            self.is_stock = False
        else:
            logger.error('Unknown exchange: {0}', self.exchange)
            assert(False)

    @classmethod
    def _get_info(cls):
        if Contract.source_type:
            return Contract.info
        # src, Contract.source_type = get_setting_datasource()
        # Contract.info = src.get_contracts()
        # return Contract.info

    @classmethod
    def from_string(cls, strcontract):
        return cls(strcontract)

    def __str__(self):
        """"""
        return "%s.%s" % (self.code, self.exchange)

    def __hash__(self):
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(self.__str__())
            return self._hash

    def __eq__(self, r):
        try:
            return self._hash == r._hash
        except AttributeError:
            return hash(self) == hash(r)

    def __cmp__(self, r):
        return str(self) < str(r)

    @classmethod
    def trading_interval(cls, contract):
        """ 获取合约的交易时段。"""
        pass

    @classmethod
    def long_margin_ratio(cls, strcontract):
        try:
            ## @todo 确保CONTRACTS.csv里面没有重复的项，否则有可能返回数组．
            return cls._get_info().loc[strcontract.upper(), 'long_margin_ratio']
        except KeyError:
            logger.warn("Can't not find contract: %s" % strcontract)
            return 1
            # assert(False)

    @classmethod
    def short_margin_ratio(cls, strcontract):
        try:
            return cls._get_info().loc[strcontract.upper(), 'short_margin_ratio']
        except KeyError:
            logger.warn("Can't not find contract: %s" % strcontract)
            return 1
            # assert(False)

    @classmethod
    def volume_multiple(cls, strcontract):
        try:
            return cls._get_info().loc[strcontract.upper(), 'volume_multiple']
        except KeyError:
            logger.warn("Can't not find contract: %s" % strcontract)
            return 1
            # assert(False)


class Period(object):
    """ 周期

    :ivar unit: 时间单位
    :ivar count: 数值
    """
    #class Type(Enum):
        #MilliSeconds = "MilliSeconds"
        #Seconds = "Seconds"
        #Minutes = "Minutes"
        #Hours = "Hours"
        #DAY = "Days"
        #Months = "Months"
        #Seasons = "Seasons"
        #Years = "Years"
    periods = {
        "MILLISECOND": 0,
        "SECOND" : 1,
        "MINUTE": 2,
        "HOUR": 3,
        "DAY": 4,
        "MONTH": 5,
        "SEASON": 6,
        "YEAR": 7
    }

    def __init__(self, strperiod):
        period = strperiod.split('.')
        if len(period) == 2:
            unit_count = int(period[0])
            time_unit = period[1].upper()
        else:
            raise PeriodTypeError
        if time_unit not in self.periods.keys():
            raise PeriodTypeError(period=time_unit)
        self.unit = time_unit
        self.count = unit_count

    def __str__(self):
        return "%d.%s" % (self.count, self.unit)

    def to_timedelta(self):
        m = {
            'DAY': 'days',
            'HOUR': 'hours',
            'MINUTE': 'minutes',
            'SECOND': 'seconds',
            'MILLISECOND': 'milliseconds',
        }
        try:
            u = m[self.unit]
            kwargs = {u: self.count}
            return timedelta(**kwargs)
        except KeyError:
            raise Exception('unit "%s" is not supported' % self.unit)

    def __cmp__(self, r):
        cmp_unit = Period.periods[self.unit]
        cmp_unit_r = Period.periods[r.unit]
        if cmp_unit < cmp_unit_r:
            return -1
        elif cmp_unit > cmp_unit_r:
            return 1
        else:
            if self.count < r.count:
                return -1
            elif self.count > r.count:
                return 1
            else:
                return 0

class PContract(object):
    """ 特定周期的合约。

    :ivar contract: 合约对象。
    :ivar period: 周期。
    """
    def __init__(self, contract, period):
        self.contract = contract
        self.period = period

    #def __str__(self):
        #""" return string like 'IF000.SHEF-10.Minutes'  """
        #return "%s-%s" % (self.contract, self.period)

    @classmethod
    def from_string(cls, strpcon):
        t = strpcon.split('-')
        return cls(Contract(t[0]), Period(t[1]))

    def __hash__(self):
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(self.__str__())
            return self._hash

    def __eq__(self, r):
        try:
            return self._hash == r._hash
        except AttributeError:
            return hash(self) == hash(r)

    def __str__(self):
        return '%s-%s' % (str(self.contract), str(self.period))

    def __cmp__(self, r):
        if self.period < r.period:
            return -1
        elif self.period > r.period:
            return 1
        else:
            if self.contract < r.contract:
                return -1
            elif self.contract > r.contract:
                return 1
            else:
                return 0



class Bar(object):
    """Bar数据。

    :ivar datetime: 开盘时间。
    :ivar open: 开盘价。
    :ivar close: 收盘价。
    :ivar high: 最高价。
    :ivar low: 最低价。
    :ivar volume: 成交量。
    """
    def __init__(self, dt, open, close, high, low, vol):
        self.datetime = dt
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = vol
