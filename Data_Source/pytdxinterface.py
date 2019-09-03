from pytdx.hq import TdxHq_API
api = TdxHq_API()
with api.connect('119.147.212.81', 7709):
    data = api.get_k_data('000001','1990-01-01','2019-08-11')
    data = api.to_df(api.get_security_bars(0, 0, '000001', 0, 10))
    # data=api.to_df(api.get_security_quotes([(0, '000001'), (1, '600300')]))
    print(data)