# -*- coding:utf-8 -*-

import tushare as ts
import random
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from Data_Source.local_cache import *
from Data_Source.datastruct import *
import matplotlib.pyplot as plt
import pandas as pd
import time
from pytdx.hq import TdxHq_API
from tushare.stock import cons as ct
import threading
from functools import partial
import tushare as ts
from pytdx.config.hosts import hq_hosts
from pytdx.util import best_ip
import multiprocessing
from constant import hq_hosts

dfcode=ts.get_stock_basics().index.tolist()
code = []
for i in dfcode[:1000]:
    if i[:6] not in ['603755', '603992', '300789', '002959', '003816', '300787', '688321', '300028']:
        code.append(i[:6])

def get_instruments(api):
    list = []
    for mrk in range(2):
        for i in range(10):
            if mrk == 1:
                list += api.get_security_list(mrk, (i + 17) * 1000)
            else:
                list += api.get_security_list(mrk, i * 1000)
    instruments = []
    for idx in api.to_df(list)['code'].tolist():
        if idx[:2] in ['60']:
            instruments.append((idx + '.SH'))
        elif idx[:2] in ['00', '30']:
            instruments.append((idx + '.SZ'))
    return instruments

def factor_adj(code):
    # url = ct.ADJ_FAC_URL%(ct.P_TYPE['http'],ct.DOMAINS['oss'], code)
    df = pd.read_csv(ct.ADJ_FAC_URL%(ct.P_TYPE['http'],
                                             ct.DOMAINS['oss'], code))
    df = df.set_index('datetime')
    return df

def get_k(list,start,end):
    api = TdxHq_API(multithread=True, raise_exception=True)
    with api.connect('119.147.212.81', 7709):
        data  = [api.get_k_data(x,start,end) for x in list]
    return data

def get_k2(list,start,end,result,client):
    result.append([client.get_k_data(x,start,end) for x in list])


def get_k3(start,end,clients,id):
    client = clients[random.randint(0,5)]
    # client = clients[0]
    data  = client.get_k_data(id,start,end)
    return data
if __name__ == '__main__':

    clients = []
    # speed = pd.DataFrame([best_ip.ping(x) for x in hq_hosts], columns=['t'])
    # ipidx = speed.sort_values('t').index.tolist()
    # cnt = min(6, len(ipidx))  # 连接6个host
    for i in range(0, 7):
        api = TdxHq_API(heartbeat=True, raise_exception=True, auto_retry=True)
        try:
            # api.connect(hq_hosts[ipidx[i]], 7709)
            api.connect(hq_hosts[i], 7709)
            clients.append(api)
        except:
            print('error')
    starttime = time.time()
    par = []
    for i in range(15):
        num = int(len(code)/15)
        par.append(code[num*i:num*i+num])
    result = []
    tasklist = []
    for i in range(15):
        try:
            client = clients[random.randint(0,6)]
            p = threading.Thread(target=get_k2, args=(par[i],'2019-08-01','2019-08-11',result,client))
            tasklist.append(p)
            p.start()
        except:
            for client in clients:
                client.disconnect()
    for task in tasklist:
        task.join()

    # t1 = threading.Thread(target=get_k2,args=(par[0],'2019-08-01','2019-08-11',result,clients[0]))
    # t2 = threading.Thread(target=get_k2,args=(par[1],'2019-08-01','2019-08-11',result,clients[1]))
    # t3 = threading.Thread(target=get_k2, args=(par[2], '2019-08-01', '2019-08-11',result, clients[2]))
    # t4 = threading.Thread(target=get_k2, args=(par[3], '2019-08-01', '2019-08-11', result,clients[3]))
    # t5 = threading.Thread(target=get_k2, args=(par[4], '2019-08-01', '2019-08-11', result,clients[4]))
    # t6 = threading.Thread(target=get_k2, args=(par[5], '2019-08-01', '2019-08-11',result, clients[5]))
    # t1.start()
    # t2.start()
    # t3.start()
    # t4.start()
    # t5.start()
    # t6.start()
    # t1.join()
    # t2.join()
    # t3.join()
    # t4.join()
    # t5.join()
    # t6.join()

    # cget_k = partial(get_k3,'2019-08-01','2019-08-11',clients)
    # ex = ThreadPoolExecutor(max_workers=1)
    # res_iter = ex.map(cget_k, code[:120])
    # for res in res_iter:  # 此时将阻塞 , 直到线程完成或异常
    #     print(res)

    # for i in code[:120]:
    #     data = clients[0].get_k_data(i,'2019-08-01','2019-08-11')
    #     print(data)
    for client in clients:
        client.disconnect()
    print(result)
    print(time.time() - starttime)



# if __name__ == '__main__':
#     api = TdxHq_API()
#     cache_path = '/Users/Eric.xu/Data/tdxcache'
#     perd = Period('1.DAY')
#     starttime = time.time()
#     with api.connect('47.103.48.45', 7709):
#         dfcode = get_instruments(api)
#         for code in dfcode:
#             if code[:6] not in ['603755','603992','300789','002959','003816','300787','688321','300028']:
#                 print('stock ID', code)
#                 fuquanfactor = factor_adj(code[:6])
#                 fuquanfactor.to_csv('/Users/Eric.xu/Data/tdxcache/'+code[:6]+'.csv')
#                 cont = Contract(code)
#                 pcont = PContract(cont, perd)
#                 datacache = CachedDatasource(TDXSource(api), LocalFsCache(cache_path))
#                 bars = datacache.get_bars(pcont, '2018/7/27', '2019/08/11')
#         # pool = Pool(4)
#         # results = pool.map(getsource, urls)
#         # pool.close()
#         # pool.join()
#         # time4 = time.time()
#     print(time.time()-starttime)

