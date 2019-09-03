# -*- coding:utf-8 -*-

import bcolz
import pandas

Data = bcolz.open('/Users/Eric.xu/.rqalpha/bundle/stocks.bcolz','a')
dic = Data.attrs
print(Data.attrs,'/n')
print(Data.shape ,'/n',Data.ndim , '/n',Data.names , '/n',Data.dtype)
pdata = Data.todataframe()
pdata.to_csv('index.csv')
print(Data.shape)