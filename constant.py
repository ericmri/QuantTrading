# -*- coding: utf-8 -*-
"""
Created on Sat Aug 01 17:28:29 2015

@author: lenovo
"""
from datetime import datetime
import pandas as pd

####################dao使用###########################
_TUSHARE_FOUNDAMENTAL_METHOD_LIST_ = ['get_growth_data', 'get_operation_data', 'get_debtpaying_data',
                                   'get_report_data', 'get_cashflow_data', 'get_profit_data']#财务获取列表
_MONGODB_ENGIN_ = 'mongodb://localhost:27017/';
_MONGODB_DATABASE_ = 'admin'




################################################
"""
时间常数
"""
_START_ = '1994-01-01';
_MIDDLE_ = '2015-11-27';
_TODAY_ = datetime.now().strftime('%Y-%m-%d');
_RATE_FREE_ = 0.05

_start_range = pd.date_range(start=_START_,periods=7)
_end_range = pd.date_range(end=_MIDDLE_,periods=7)

"""
数据库常数
"""
_PATH_CODE_ = 'd:/data/code.csv';
_ENGINE_ = 'postgresql://postgres:root@localhost:5432/tushare'
XRDR_FACTOR_PATH = '/Users/Eric.xu/PycharmProjects/ATrading/fqfactors/'
XRDRTDXPATH = '/Users/Eric.xu/PycharmProjects/ATrading/data/xrxd/'
CALENDA_PATH = '/Users/Eric.xu/PycharmProjects/ATrading/data/calendar.csv'
TUSHARE_TOKEN = 'ee38c30341daff4f0ff0ad666f8668af0cc1796c591b638a89aed63c'

#数据库参数信息及基础语句，pgres——test用
_DATABASE_ = 'tushare'
_USER_ =  'postgres'
_PASSWORD_ = 'root'
_HOST_ =  '127.0.0.1'

_LOG_FILENAME_ = 'logging.conf' #日志配置文件名
_LOG_CONTENT_NAME_ = 'pg_log' #日志语句提示信息

# server info
# SERVERINFO = ('47.103.48.45',7709)
SERVERINFO = ('119.147.212.81',7709)
# hq_hosts = ['47.103.48.45','123.125.108.90','175.6.5.153','182.118.47.151','182.131.3.245','202.100.166.27''42.123.69.62',
    #             '222.161.249.156','58.63.254.191','58.63.254.217']
hq_hosts = ['47.103.48.45','47.103.86.229','47.103.88.146','120.79.60.82','47.112.129.66','39.98.234.173','39.98.198.249'
            ,'39.100.68.59']