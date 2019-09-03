import tushare as ts
import pandas as pd

token = 'ee38c30341daff4f0ff0ad666f8668af0cc1796c591b638a89aed63c'
path = '/Users/Eric.xu/PycharmProjects/ATrading/data/'
#提取000001全部复权因子
# print(pd.read_pickle('/Users/Eric.xu/PycharmProjects/ATrading/fqfactors/factor.pkl'))
pro = ts.pro_api(token)
# df = pro.adj_factor(ts_code='000001.SZ', trade_date='')
# date = pro.trade_cal()
# tradecalendar = pro.trade_cal()
# tradecalendar.to_csv('/Users/Eric.xu/PycharmProjects/ATrading/data/'+'calendar.csv')
stocklistdf = pro.stock_basic()
stocklistdf.to_pickle(path + 'stocklist.pkl')
stocklist = stocklistdf['ts_code']
dflist = []
for id in stocklist:
    # df = pro.adj_factor(ts_code = id)
    # df.to_csv(path + "/" + id[:6] + ".csv" )
    df = pd.read_csv(path + "/" + id[:6] + ".csv" )
    dflist.append(df)
result = pd.concat(dflist)
result.to_pickle(path + "/" + 'factor.pkl')

