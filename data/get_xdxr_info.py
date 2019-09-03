# coding=utf-8

from pytdx.parser.base import BaseParser
from pytdx.helper import get_datetime, get_volume, get_price, get_time
from collections import OrderedDict
import struct
import six
import pytdx
from constant import XRDR_FACTOR_PATH, TUSHARE_TOKEN, CALENDA_PATH
import pandas as pd
import datetime
"""
need to fix

get_volume ?

4098 ---> 3.0

2434.0062499046326 ---> 2.6

1218.0031249523163 ---> 2.3

"""
"""

        1 除权除息 002175 2008-05-29
        2 送配股上市  000656 2015-04-29
        3 非流通股上市 000656 2010-02-10
        4 未知股本变动 600642 1993-07-19
        5 股本变化 000656 2017-06-30
        6 增发新股 600887 2002-08-20
        7 股份回购  600619 2000-09-08
        8 增发新股上市 600186 2001-02-14
        9 转配股上市 600811 2017-07-25
        10 可转债上市 600418 2006-07-07
        11 扩缩股  600381 2014-06-27
        12 非流通股缩股 600339 2006-04-10
        13 送认购权证 600008 2006-04-19
        14 送认沽权证 000932 2006-03-01
        #         ('year', year),
        #         ('month', month),
        #         ('day', day),
        #         ('category', category),
        #         ('name', self.get_category_name(category)),
        #         ('fenhong', fenhong),
        #         ('peigujia', peigujia),
        #         ('songzhuangu', songzhuangu),
        #         ('peigu', peigu),
        #         ('suogu', suogu),
        #         ('panqianliutong', panqianliutong),
        #         ('panhouliutong', panhouliutong),
        #         ('qianzongguben', qianzongguben),
        #         ('houzongguben', houzongguben),
        #         ('fenshu', fenshu),
        #         ('xingquanjia', xingquanjia)

"""


XDXR_CATEGORY_MAPPING = {
    1 : "除权除息",
    2 : "送配股上市",
    3 : "非流通股上市",
    4 : "未知股本变动",
    5 : "股本变化",
    6 : "增发新股",
    7 : "股份回购",
    8 : "增发新股上市",
    9 : "转配股上市",
    10 : "可转债上市",
    11 : "扩缩股",
    12 : "非流通股缩股",
    13 : "送认购权证",
    14 : "送认沽权证"
}


class GetXdXrInfo():

    def __init__(self,api,XRDR_FACTOR_PATH):
        self.__api = api
        self.xdxr_factor_path = XRDR_FACTOR_PATH

    def is_trading_day(self,date):
        tradingdate = pd.read_csv(CALENDA_PATH)
        rst = True if tradingdate['cal_date' == date].is_open.values[-1] else False
        return rst

    def cal_fuquanfactor(self,price,fenhong,songgu,peigu,peigujia): #计算后复权因子
        print('calculating fuquanfactor')
        factor = price*(1.0+peigu+songgu)/(price - fenhong + peigu*peigujia)
        return factor

    def get_xrdr_para(self,row):
        row = row.fillna('novalue')
        fenhong = row['fenhong']/10.0 if row['fenhong'] != 'novalue'  else 0
        songgu = row['songzhuangu']/10.0 if row['songzhuangu'] != 'novalue' else 0
        peigu = row['peigu']/10.0 if row['peigu'] != 'novalue' else 0
        peigujia = row['peigujia'] if row['peigujia'] != 'novalue' else 0
        return fenhong,songgu,peigu,peigujia

    def get_instruments(self):
        list = []
        for mrk in range(2):
            for i in range(10):
                if mrk == 1:
                    list += self.__api.get_security_list(mrk, (i + 17) * 1000)
                else:
                    list += self.__api.get_security_list(mrk, i * 1000)
        instruments = []
        for idx in self.__api.to_df(list)['code'].tolist():
            if idx[:2] in ['60']:
                instruments.append((1, idx,'.SH'))
            elif idx[:2] in ['00', '30']:
                instruments.append((0, idx,'.SZ'))
        return instruments

    def get_price(self,instrument,date):
        startdate = date - datetime.timedelta(days = 20)
        startdate = startdate.strftime("%Y-%m-%d")
        date = date.strftime("%Y-%m-%d")
        price = self.__api.get_k_data(instrument,startdate,date)
        rst = price.close.values[-2]
        return rst


    def update_xrxd_factor(self,instruments,tdx_xrdr_info):
        updatedate = []
        for instrument in instruments:
            localxrdrfactor  = pd.read_csv(self.xdxr_factor_path+instrument+'.csv')
            localfuqianfactor = localxrdrfactor.adj_factor.values[0]
            localfactordate = datetime.datetime.strptime(str(localxrdrfactor.trade_date.values[0]),'%Y%m%d') #获取本地最新的XRDR日期
            localfactordate = datetime.datetime.strptime('20160607','%Y%m%d') #for debugging only
            tdx_xrdr_info['date'] = pd.to_datetime(tdx_xrdr_info['date'])
            tdxxrdrinfo = tdx_xrdr_info[(tdx_xrdr_info.code == int(instrument)) & (tdx_xrdr_info.date > localfactordate)]
            factor = 1
            newxrdrfactors = pd.DataFrame()
            for index, row in tdxxrdrinfo.iterrows():
                if row['category'] == 1.0:
                    fenhong, songgu, peigu, peigujia = self.get_xrdr_para(row)
                    price = self.get_price(instrument,row['date'])
                    factor = factor*self.cal_fuquanfactor(price,fenhong,songgu,peigu,peigujia)
                    newxrdrfactors = newxrdrfactors.append({'ts_code':instrument,'trade_date':row['date'],
                                                              'adj_factor':factor*localfuqianfactor},ignore_index=True)
            newxrdrfactors = newxrdrfactors.sort_values(by = 'trade_date',ascending=False)
            localxrdrfactor = newxrdrfactors.append(localxrdrfactor)
        return localxrdrfactor

    def update_xrxd_ts(self):
        import tushare as ts
        print('update srdrfactors from tushare')
        xrdrfactors = pd.DataFrame()
        pro = ts.pro_api(TUSHARE_TOKEN)
        stocklist = self.get_instruments()
        for id in stocklist:
            if id[0] == 1:
                ts_code = id[1]+'.SH'
            else:
                ts_code = id[1] + '.SZ'
            xrdrfactors = xrdrfactors.append(pro.adj_factor(ts_code = ts_code))
        xrdrfactors.to_csv(XRDR_FACTOR_PATH + "tusharexrdr.csv" )

    def load_tdx_xrdrinfo(self,filename):
        xrdrinfo = pd.read_csv('/Users/Eric.xu/PycharmProjects/ATrading/data/xrxd/' + filename)
        return xrdrinfo


    def get_category_name(self, category_id):

        if category_id in XDXR_CATEGORY_MAPPING:
            return XDXR_CATEGORY_MAPPING[category_id]
        else:
            return str(category_id)


    def get_tdx_xrdrinfo(self):
        list = self.get_instruments(self.__api)
        idlist = []
        rst = pd.DataFrame()
        for idx in self.__api.to_df(list)['code'].tolist():
            # data = api.to_df(api.get_k_data(idx , '2019-08-05', '2019-08-05'))
            if idx[:2] in ['60']:
                idlist.append((1, idx))
            elif idx[:2] in ['00', '30']:
                idlist.append((0, idx))
                # time.sleep(0.001)
        for i in idlist:
            result = self.__api.to_df(self.__api.get_xdxr_info(i[0], i[1]))
            result['code'] = i[1]
            data = rst.append(result)
        data[['year', 'month', 'day']] = data[['year', 'month', 'day']].astype(int)
        data[['year', 'month', 'day']] = data[['year', 'month', 'day']].astype(str)
        data['date'] = data['year'] + '-' + data['month'] + '-' + data['day']
        data['date'] = pd.to_datetime(data['date'])
        return data

    def data_stock_to_fq(self,bfq_data, xdxr_data, fqtype):
        '使用数据库数据进行复权'
        info = xdxr_data.query('category==1')
        bfq_data = bfq_data.assign(if_trade=1)

        if len(info) > 0:
            data = pd.concat(
                [
                    bfq_data,
                    info.loc[bfq_data.index[0]:bfq_data.index[-1],
                    ['category']]
                ],
                axis=1
            )

            data['if_trade'].fillna(value=0, inplace=True)
            data = data.fillna(method='ffill')

            data = pd.concat(
                [
                    data,
                    info.loc[bfq_data.index[0]:bfq_data.index[-1],
                    ['fenhong',
                     'peigu',
                     'peigujia',
                     'songzhuangu']]
                ],
                axis=1
            )
        else:
            data = pd.concat(
                [
                    bfq_data,
                    info.
                        loc[:,
                    ['category',
                     'fenhong',
                     'peigu',
                     'peigujia',
                     'songzhuangu']]
                ],
                axis=1
            )
        data = data.fillna(0)
        data['preclose'] = (
                                   data['close'].shift(1) * 10 - data['fenhong'] +
                                   data['peigu'] * data['peigujia']
                           ) / (10 + data['peigu'] + data['songzhuangu'])

        if fqtype in ['01', 'qfq']:
            data['adj'] = (data['preclose'].shift(-1) /
                           data['close']).fillna(1)[::-1].cumprod()
        else:
            data['adj'] = (data['close'] /
                           data['preclose'].shift(-1)).cumprod().shift(1).fillna(1)

        for col in ['open', 'high', 'low', 'close', 'preclose']:
            data[col] = data[col] * data['adj']
        data['volume'] = data['volume'] / \
                         data['adj'] if 'volume' in data.columns else data['vol'] / data['adj']
        try:
            data['high_limit'] = data['high_limit'] * data['adj']
            data['low_limit'] = data['high_limit'] * data['adj']
        except:
            pass
        return data.query('if_trade==1 and open != 0').drop(
            ['fenhong',
             'peigu',
             'peigujia',
             'songzhuangu',
             'if_trade',
             'category'],
            axis=1,
            errors='ignore'
        )

    def QA_data_stock_to_fq(self,__data, type_='01'):

        def __QA_fetch_stock_xdxr(
                code,
                xrdrinfo,
                format_='pd'
        ):
            '获取股票除权信息/数据库'
            try:
                data = pd.DataFrame(
                    [item for item in xrdrinfo]
                ).drop(['_id'],
                       axis=1)
                data['date'] = pd.to_datetime(data['date'])
                return data.set_index(['date', 'code'], drop=False)
            except:
                return pd.DataFrame(
                    data=[],
                    columns=[
                        'category',
                        'category_meaning',
                        'code',
                        'date',
                        'fenhong',
                        'fenshu',
                        'liquidity_after',
                        'liquidity_before',
                        'name',
                        'peigu',
                        'peigujia',
                        'shares_after',
                        'shares_before',
                        'songzhuangu',
                        'suogu',
                        'xingquanjia'
                    ]
                )

        '股票 日线/分钟线 动态复权接口'

        code = __data.index.remove_unused_levels().levels[1][0] if isinstance(
            __data.index,
            pd.core.indexes.multi.MultiIndex
        ) else __data['code'][0]

        return self.data_stock_to_fq(
            bfq_data=__data,
            xdxr_data=__QA_fetch_stock_xdxr(code),
            fqtype=type_
        )

if __name__ == '__main__':
    from pytdx.hq import TdxHq_API
    import time
    api = TdxHq_API()
    starttime = time.time()
    with api.connect('47.103.48.45', 7709):
        xrdr = GetXdXrInfo(api,XRDR_FACTOR_PATH)
        localtdxinfo = xrdr.load_tdx_xrdrinfo('xrxdinfo00.csv')
        # xrdrinfobycode = localtdxinfo.query("code== '600050'")
        xrdrinfobycode = localtdxinfo[localtdxinfo['code'] == 1]
        xrdrinfobycode[['year','month','day']] = xrdrinfobycode[['year','month','day']].astype(int)
        xrdrinfobycode[['year','month','day']] = xrdrinfobycode[['year','month','day']].astype(str)
        xrdrinfobycode['datetime'] = xrdrinfobycode['year']+'-'+xrdrinfobycode['month']+'-'+xrdrinfobycode['day']
        xrdrinfobycode['datetime'] = pd.to_datetime(xrdrinfobycode['datetime'],format = "%Y-%m-%d").dt.date
        # year = xrdrinfobycode['year']
        # month = xrdrinfobycode['month']
        # day = xrdrinfobycode['day']
        # xrdrinfobycode['datetiem'] = datetime.datetime(year,month,day)
        kdata = api.get_k_data('000001','1990-01-01','2019-08-14')
        kdata = kdata.set_index('date')
        xrdrinfobycode = xrdrinfobycode.set_index('datetime')
        # xrdrinfobycode.set_index('date')
        data = pd.concat([kdata,xrdrinfobycode], axis=1,join_axes=[kdata.index])
        # data = xrdr.update_xrxd_factor(['000001'],localtdxinfo)
    print(time.time() - starttime)


    #     starttime = time.time()
    #     xrdr = GetXdXrInfo(api,XRDR_FACTOR_PATH)
        # get_quotation2(api)
        # index_time = api.to_df(api.get_index_bars(9, 1, '000001', 0, 2))
        # b= api.to_df(api.get_security_bars(9, 0, '000043', 0, 100))
        # c= api.to_df(api.get_k_data(9, 0, '000001', '2019-08-05', '2019-08-05'))
        # data = api.to_df(api.get_xdxr_info(1, '600827'))
        # data = xrdr.get_tdx_xrdrinfo()
        # data.to_csv('/Users/Eric.xu/PycharmProjects/ATrading/data/xrxd/'+'600827.csv')
        # print(data)
        # data.to_csv('/Users/Eric.xu/PycharmProjects/ATrading/data/xrxd/' + 'xrxdinfo.csv')
        # deltat = time.time() - starttime

