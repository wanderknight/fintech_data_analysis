__author__ = 'wanderknight'
__time__ = '2018/5/11 19:02'

transmission_relation = {('37#A11.csv', 'SH#999999.csv'): 1,
                         ('SH#999999.csv', 'SZ#000002.csv'): 1,
                         ('SH#999999.csv', 'SZ#300001.csv'): 1,
                         ('SH#999999.csv', 'SH#600004.csv'): 0,
                         ('SH#999991.csv', 'SH#600005.csv'): 0}

follow_assets = {'SZ#000002.csv': ['user_a1', 'user_a2'],
                 'SZ#300001.csv': [],
                 'SH#600004.csv': ['user_a1']}

path_dict = transmission_relation
high_volatility_asset = '37#A11.csv'

asset_list = [x[0] for x in path_dict.keys()]
transmited_assets = []
transmited_assets.append(high_volatility_asset)
transmission_path = [[high_volatility_asset]]

if high_volatility_asset in asset_list:
    for hva in transmited_assets:
        for pa in path_dict.keys():
            if hva == pa[0]:
                transmited_assets.append(pa[1])
                # 生成相关路径
                for tp in transmission_path:
                    if tp[-1] == hva:
                        transmission_path.append(tp + [pa[1]])

del transmited_assets[0]
# 输出生成的波动资产传输路径
for tp in transmission_path:
    print(tp)

# 根据关注人关注的资产，输出关注资产波动的相关路径
follow_user = 'user_a3'
#关注的影响资产
transmited_follow_asset = set(transmited_assets) & set(follow_assets.keys())

for tra in transmited_follow_asset:
    if follow_user in follow_assets[tra]:
        for tp in transmission_path:
            if tra == tp[-1]:
                print(follow_user,"关注的",tp,"大波动")