from six import PY2
from pytdx.hq import TdxHq_API
from concurrent.futures import ThreadPoolExecutor
import asyncio
import tushare as ts
code=ts.get_stock_basics().index.tolist()
best_ip='119.147.212.81'




if not PY2:
    import queue
    from datetime import datetime


    class ConcurrentApi:
        def __init__(self, *args, **kwargs):
            self.thread_num = kwargs.pop('thread_num', 4)
            self.ip = kwargs.pop('ip', '119.147.212.81')
            self.executor = ThreadPoolExecutor(self.thread_num)

            self.queue = queue.Queue(self.thread_num)
            for i in range(self.thread_num):
                api = TdxHq_API(args, kwargs)
                api.connect(self.ip)
                self.queue.put(api)

        def __getattr__(self, item):
            api = self.queue.get()
            func = api.__getattribute__(item)

            def wrapper(*args, **kwargs):
                res = self.executor.submit(func, *args, **kwargs)
                self.queue.put(api)
                return res

            return wrapper


    # 获取股票列表，并行版
    def concurrent_api(num=4):
        capi = ConcurrentApi(thread_num=num)
        now = datetime.now()
        data = {capi.get_security_list(0, 100) for i in range(100)}
        dd = [i.result() for i in data]
        return (datetime.now() - now).total_seconds()


    # 获取股票列表，原生版
    def original_api():
        api = TdxHq_API()
        api.connect()
        now = datetime.now()
        dd = [api.get_security_list(0, 100) for i in range(100)]
        return (datetime.now() - now).total_seconds()


    # 获取全市场行情，并行版
    def concurrent_quotes(num=4):
        capi = ConcurrentApi(thread_num=num, ip='47.103.48.45')
        now = datetime.now()
        data = {capi.get_security_quotes(
            code[80 * pos:80 * (pos + 1)]) for pos in range(int(len(code) / 80) + 1)}

        dd = [i.result() for i in data]
        return (datetime.now() - now).total_seconds()


    # 获取全市场行情，原生版
    def original_quotes():
        api = TdxHq_API()
        api.connect('47.103.48.45')
        now = datetime.now()
        data = [api.get_security_quotes(
            code[80 * pos:80 * (pos + 1)]) for pos in range(int(len(code) / 80) + 1)]
        return (datetime.now() - now).total_seconds()


    def get_market(code):
        code = str(code)
        # if code[0] in ['5', '6', '9'] or code[:3] in ["009", "126", "110", "201", "202", "203", "204"]:
        #     return 1
        # return 0
        if code[0] in ['6']:
            return 1
        elif code[0] in ['0','3']:
            return 0

    def concurrent_k_data(num=1):
        capi = ConcurrentApi(thread_num=num, ip=best_ip)
        now = datetime.now()
        data = {capi.get_security_bars(9,get_market(x) ,x,0,100) for x in code[:90]}
        # data = capi.get_security_bars(9, get_market(x), 0, 100)
        #print(data)
        dd = [i.result() for i in data]
        print((datetime.now() - now).total_seconds())
        return dd


    def concurrent_quotes2(num=2):
        capi = ConcurrentApi(thread_num=num, ip=best_ip)
        now = datetime.now()
        list = []
        a = list.append([(get_market(x), x) for x in code[90 * pos:90 * (pos + 1)]] for pos in range(int(len(code) / 90) + 1))
        data = {capi.get_security_quotes([(get_market(x), x) for x in code[90 * pos:90 * (pos + 1)]]) for pos in
                range(int(len(code) / 90) + 1)}
        # print(data)
        dd = [i.result() for i in data]
        print((datetime.now() - now).total_seconds())
        return dd

if __name__ == '__main__':
    data = concurrent_k_data(2)
