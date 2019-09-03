# -*- coding:utf-8 -*-

import os, logging
import pickle
import pandas as pd
import six
import tushare as ts
from pytdx.hq import TdxHq_API
import datetime
import queue
import time
from concurrent.futures import ThreadPoolExecutor,as_completed
# from datetime import datetime, timedelta
from Data_Source.datastruct import *

logger = logging.Logger('data')
# dfcode=ts.get_stock_basics().index.tolist()
# code = []
# for i in dfcode:
#     if i[:6] not in ['603755', '603992', '300789', '002959', '003816', '300787', '688321', '300028']:
#         code.append(i[:6])

# code = code[:100]
stock_ip_list = ['47.103.48.45','47.103.86.229','47.103.88.146','120.79.60.82','47.112.129.66','39.98.234.173','39.98.198.249'
            ,'39.100.68.59']

class _HashObjectImpl(object):
    def __str__(self):
        keys = filter(lambda k: not k.startswith('__'), dir(self))
        d = {}
        for k in keys:
            d[k] = getattr(self, k)
        return str(d)

class HashObject(object):
    @staticmethod
    def new(**kwargs):
        obj = _HashObjectImpl()
        for k, v in six.iteritems(kwargs):
            setattr(obj, k, v)
        return obj

class SourceWrapper(object):
    """ 数据源包装器，使相关数据源支持逐步读取操作 """

    def __init__(self, pcontract, data,  max_length):
        self.data = data
        self.curbar = -1
        self.pcontract = pcontract
        self._max_length = max_length

    def __len__(self):
        return self._max_length

    def rolling_forward(self):
        """ 读取下一个数据"""
        self.curbar += 1
        if self.curbar == self._max_length:
            return False, self.curbar
        else:
            return True, self.curbar
    def get_id(self):
        return self.pcontract

    def get_bars(self,len):
        return self.data['close'][-len:]

def _process_ts_dt(dt):
    return str(pd.to_datetime(dt))

class DatasourceAbstract(object):
    '''数据源抽象基类'''

    def get_bars(self, pcontract, dt_start, dt_end):
        raise NotImplementedError

    def get_last_bars(self, pcontract, n):
        raise NotImplementedError

    def get_contracts(self):
        raise NotImplementedError

    def get_code2strpcon(self):
        raise NotImplementedError

class CacheAbstract(DatasourceAbstract):
    '''缓存抽象类'''

    def save_data(self, data, pcontract):
        raise NotImplementedError


class LoadCacheException(Exception):
    def __init__(self, missing_range, cached_data=None):
        assert(missing_range)
        self.cached_data = cached_data
        self.missing_range = missing_range

    def __str__(self):
        return str(self.missing_range)

class CachedDatasource(DatasourceAbstract):
    '''带缓存的数据源'''

    def __init__(self, datasource, cache,thread_num,sleep_time = 10,timeout = 3):
        self.datasource = datasource
        self.cache = cache
        self.thread_num = thread_num
        self._queue = queue.Queue(maxsize=50)
        self.api_no_connection = TdxHq_API()
        # self._api_worker = Thread(
        #     target=self.api_worker, args=(), name='API Worker')
        # self._api_worker.start()
        # self._api_worker.join()
        # time.sleep(10)
        self.timeout = timeout
        self.executor = ThreadPoolExecutor(self.thread_num)
        self.sleep_time = sleep_time

    def _test_speed(self, ip, port=7709):

        api = TdxHq_API(raise_exception=True, auto_retry=False)
        _time = datetime.datetime.now()
        # print(self.timeout)
        # print('in testspeed')
        try:
            with api.connect(ip, port):
                res = api.get_security_list(0, 1)
                print('res',ip)
                # print(len(res))
                if len(res) > 800:
                    return (datetime.datetime.now() - _time).total_seconds()
                else:
                    return datetime.timedelta(9, 9, 0).total_seconds()
        except Exception as e:
            return datetime.timedelta(9, 9, 0).total_seconds()

    def get_available(self):
        if self._queue.empty() is False:
            api = self._queue.get_nowait()
            print('getting avaliable api',api)
            return api
        else:
            # Timer(0, self.api_worker).start()
            # print('start timer threading')
            # return self._queue.get()
            print('tdx api pool in empty')

    def api_worker(self):
        data = []
        if self._queue.qsize() < 20:
            for item in stock_ip_list:
                # print('item, api_worker',item)
                if self._queue.full():
                    # print('apiworkfull')
                    break
                _sec = self._test_speed(ip=item, port=7709)
                # print('testspeed',_sec)
                if _sec < self.timeout * 3:
                    # print('put api in queue',item)
                    try:
                        self._queue.put(TdxHq_API(heartbeat=False).connect(
                            ip=item, port=7709))

                    except:
                        logger('error_api_worker')

    def get_bars(self, pcontract, dt_start, dt_end):
        try:
            logger.info('trying to load from cache')
            return self.cache.get_bars(pcontract, dt_start, dt_end)
        except LoadCacheException as e:
            api = self.get_available()
            logger.info('updating cache')
            missing_range = e.missing_range
            logger.info('missing range: {0}', missing_range)
            missing_data = []
            for start, end in missing_range:
                wrapper = self.datasource.get_bars(pcontract, start, end, api)
                if wrapper is None:
                    self._queue.put(api)
                    return
                else:
                    missing_data.append(HashObject.new(data=wrapper.data,
                                                       start=start,
                                                       end=end))
                    self._queue.put(api)
            self.cache.save_data(missing_data, pcontract)
            logger.info('loading cache')
            return self.cache.get_bars(pcontract, dt_start, dt_end)

    def get_last_bars(self, pcontract, n):
        raise NotImplementedError

    def get_contracts(self):
        # TODO:
        return self.datasource.get_contracts()

class TuShareSource(DatasourceAbstract):
    '''TuShare数据源'''

    def __init__(self):

       self.cons = 1
       #  self.cons = ts.get_apis()

    def get_bars(self, pcontract, dt_start, dt_end):
        # TODO: 判断pcontract是不是股票，或者tushare支持期货？
        data = self._load_data(pcontract, dt_start, dt_end)
        assert data.index.is_unique
        return SourceWrapper(pcontract, data, len(data))

    def get_last_bars(self, pcontract, n):
        # TODO
        pass

    def _load_data(self, pcontract, dt_start, dt_end):
        cons = ts.get_apis()
        # cons = self.cons
        dts = _process_ts_dt(dt_start)
        dte = _process_ts_dt(dt_end)
        # data = ts.get_k_data(pcontract.contract.code, start=dts, end=dte)
        data = ts.bar(pcontract.contract.code, conn = cons, start_date=dts, end_date=dte, adj = 'qfq',retry_count=6)
        # data.set_index('date',drop=True,inplace=True)
        data.index.names = ['datetime']
        ts.close_apis(cons)
        return data.iloc[::-1]

    def get_contracts(self):
        # TODO
        return pd.DataFrame()

class TDXSource(DatasourceAbstract):
    '''TDX数据源'''

    def get_bars(self, pcontract, dt_start, dt_end,api):
        # TODO: 判断pcontract是不是股票，或者tushare支持期货？
        data = self._load_data(pcontract, dt_start, dt_end,api)
        if data is None:
            print('no data available for ', pcontract)
            return
        else:
            assert data.index.is_unique
            return SourceWrapper(pcontract, data, len(data))


    def get_last_bars(self, pcontract, n):
        # TODO
        pass

    def _load_data(self, pcontract, dt_start, dt_end,api):
        dts = _process_ts_dt(dt_start)[:10]
        dte = _process_ts_dt(dt_end)[:10]
        # data = ts.get_k_data(pcontract.contract.code, start=dts, end=dte)
        try:
            data = api.get_k_data(pcontract.contract.code, start_date=dts, end_date=dte,)
            data.drop(['date'], axis=1, inplace=True)
            data.index.names = ['datetime']
            return data.iloc[::-1]
        except:
            print('error while dowloading data', pcontract)
            return
        # data.set_index('date',drop=True,inplace=True)
        # data.rename(columns={'date': 'datetime'}, inplace=True)


    def get_contracts(self):
        # TODO
        return pd.DataFrame()

class LocalFsCache(CacheAbstract):
    '''本地文件系统缓存'''

    def __init__(self, base_path):
        self._base_path = base_path
        self._load_meta()

    def get_bars(self, pcontract, dt_start, dt_end):
        key = self._to_key(pcontract)
        dt_start = pd.to_datetime(dt_start)
        dt_end = pd.to_datetime(dt_end)
        try:
            cached_start, cached_end = self._meta[key]
            missing_range = _missing_range(
                pcontract.period.to_timedelta(),
                dt_start, dt_end, cached_start, cached_end)
            data = self._load_data_from_path(self._key_to_path(key))
            if missing_range:
                raise LoadCacheException(missing_range, data)
            data = _filter_by_datetime_range(data, dt_start, dt_end)
            return SourceWrapper(pcontract, data, len(data))
        except (KeyError, IOError):
            raise LoadCacheException([(dt_start, dt_end)])

    def save_data(self, missing_data, pcontract):
        key = self._to_key(pcontract)
        path = self._key_to_path(key)
        data_arr = list(map(lambda t: t.data, missing_data))
        try:
            old_data = self._load_data_from_path(path)
            data_arr.insert(0, old_data)
        except IOError:
            pass
        data = _merge_data(data_arr)
        self._save_data_to_path(data, path)
        self._update_meta(key, map(lambda t: (t.start, t.end), missing_data))
        self._save_meta()

    def _check_base_path(self):
        if not os.path.isdir(self._base_path):
            os.makedirs(self._base_path)

    def _load_data_from_path(self, path):
        return pd.read_csv(path, index_col=0, parse_dates=True)

    def _save_data_to_path(self, data, path):
        self._check_base_path()
        data.to_csv(path)

    def _meta_path(self):
        return os.path.join(self._base_path, '_meta')

    def _load_meta(self):
        try:
            with open(self._meta_path(), 'rb') as f:
                self._meta = pickle.load(f)
        except IOError:
            self._meta = {}

    def _save_meta(self):
        self._check_base_path()
        with open(self._meta_path(), 'wb') as f:
            pickle.dump(self._meta, f, protocol=2)

    def _update_meta(self, key, range_lst):
        starts, ends = map(lambda t: list(t), zip(*range_lst))
        try:
            cached_start, cached_end = self._meta[key]
            starts.append(cached_start)
            ends.append(cached_end)
        except KeyError:
            pass
        new_start = None if any(map(lambda d: d is None, starts)) \
            else min(starts)
        new_end = max(ends)
        self._meta[key] = new_start, new_end

    def _to_key(self, pcontract):
        return str(pcontract)

    def _key_to_path(self, key):
        path = os.path.join(self._base_path, key + '.csv')
        return path


def _merge_data(arr):
    # a = pd.concat(arr)
    # b= a.reset_index()
    # c = b.drop_duplicates('datetime', keep='last')
    # d= c.set_index('datetime').sort_index()
    return pd.concat(arr)\
             .reset_index()\
             .drop_duplicates('datetime', keep='last')\
             .set_index('datetime').sort_index()


def _missing_range(delta, dt_start, dt_end, cached_start, cached_end):
    result = []
    if cached_start is not None:
        if dt_start is None or dt_start < cached_start:
            result.append((dt_start, cached_start - delta))
    if dt_end > cached_end:
        result.append((cached_end + delta, dt_end))
    return result


def _filter_by_datetime_range(data, start, end):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    if start is None:
        if end is None:
            return data
        else:
            return data[data.index <= end]
    else:
        if end is None:
            return data[data.index >= start]
        else:
            return data[(data.index >= start) & (data.index <= end)]


if __name__ == '__main__':
    stockpool = ts.get_stock_basics().sort_values('timeToMarket')
    stockpool = stockpool[(stockpool['timeToMarket']) > 0]
    dfcode = stockpool.index.tolist()
    stockcode=[id[:6]+'.SH' if id[-6]=='6' else id[:6]+'.SZ' for id in dfcode]
    cache_path = '/Users/Eric.xu/Data/cacheddata'
    perd = Period('1.DAY')
    datacache = CachedDatasource(TDXSource(), LocalFsCache(cache_path), thread_num=4)
    datacache.api_worker()
    dts = '2019/7/25'
    dte = '2019/08/12'
    startt = time.time()
    bars = {datacache.executor.submit(datacache.get_bars,PContract(Contract(code),perd),dts,dte) for code in stockcode}
    data = [task.result() for task in as_completed(bars)]

    print(data[0].get_bars(2))

    print(time.time()-startt)