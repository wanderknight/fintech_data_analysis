__author__ = 'wanderknight'
__time__ = '2018/5/15 10:33'
from py2neo import Graph, Node, Relationship


def return_hy_def(path, hy_def_type):
    """
    :param path: incon.dat file position
    :param hy_def_type: defined type:'ZJHHY', 'TDXNHY', 'SWHY', 'HKHY', 'MGHY', 'FUND_LB', 'FUND_TZLB',
                                    'FUND_TZStyle', 'FUND_Company', 'SBHY_TZ', 'SBQS_TZ'
    :return:list[list],所有行业分类，每个内部list保存行业编号和名称。
    """
    hy_dict = {}
    with open(path) as f:
        for line in f.readlines():
            """###### 结束标志
                #TDXNHY 开始标志"""
            if '#' in line and line.strip().strip('#') != '':  # 处理结束标志和开始标志
                temp_key = line.strip().strip('#')
                hy_dict[temp_key] = []
            elif line.strip() != '######':
                hy_dict[temp_key].append(line.strip().split('|'))
    return hy_dict[hy_def_type]


def init_hy_transmission_graph(graph, hy_list):
    """
    创建行业相关图，create the index node and relationship
    :param graph:
    :param hy_list:
    :return:
    """
    root_note = Node('china_index', name='中国市场指数')
    graph.create(root_note)
    for i, list_item in enumerate(hy_list):
        if 'T01' in list_item[0] or 'T10' in list_item[0]:  # for test
            index_node = Node("TDX_index", name=list_item[1], tdx_hy_code=list_item[0])
            graph.create(index_node)
            # print(list_item[0])
            if len(list_item[0]) > 3:
                # get father tdx_hy_code and the node
                for j in range(i - 1, -1, -1):
                    if hy_list[j][0] in list_item[0]:
                        father_hy_code = hy_list[j][0]
                        father_node = graph.find_one('TDX_index', property_key='tdx_hy_code',
                                                     property_value=father_hy_code)
                        relat_transmission = Relationship(father_node, 'Transmission', index_node)
                        # print(relat_transmission)
                        break
            else:
                relat_transmission = Relationship(root_note, 'Transmission', index_node)
            graph.create(relat_transmission)


def append_hy_graph_index(path, graph):
    """
    为行业相关图添加相关的tdx指数
    :param path:
    :param graph:
    :return:
    """
    with open(path) as f:
        for line in f.readlines():
            line_list = line.strip().split('|')
            if 'T01' in line_list[-1] or 'T10' in line_list[-1]:
                print(line_list[-1])
                temp_node = graph.find_one('TDX_index', property_key='tdx_hy_code', property_value=line_list[-1])
                temp_node['ref_index'] = line_list[1]
                graph.push(temp_node)


def init_stock_transmission_graph(path, graph):
    with open(path) as f:
        for line in f.readlines():
            line_list = line.strip().split('|')
            if 'T01' in line_list[2] or 'T10' in line_list[2]:
                father_node = graph.find_one('TDX_index', property_key='tdx_hy_code', property_value=line_list[2])
                stock_node = Node('stock', tdx_stock_code=line_list[1])
                graph.create(stock_node)
                relat_transmission = Relationship(father_node, 'Transmission', stock_node)
                graph.create(relat_transmission)


def add_event_node(graph):
    # 添加宏观经济对金融业影响 rrr.csv
    event_node = Node('event', event_type='rrr')
    graph.create(event_node)
    hy_node = graph.find_one('TDX_index', property_key='tdx_hy_code', property_value='T10')
    relat_transmission = Relationship(event_node, 'Transmission', hy_node)
    graph.create(relat_transmission)
    # 添加能源T007
    event_node = Node('event', event_type='T007')
    graph.create(event_node)
    hy_node = graph.find_one('TDX_index', property_key='tdx_hy_code', property_value='T01')
    relat_transmission = Relationship(event_node, 'Transmission', hy_node)
    graph.create(relat_transmission)


test_graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="neo4j"
)
incon_dat_path = 'D:\\zd_zxjtzq\\incon.dat'
tdxzs_cfg_path = 'D:\\zd_zxjtzq\\T0002\\hq_cache\\tdxzs.cfg'
tdxhy_cfg_path = 'D:\\zd_zxjtzq\\T0002\\hq_cache\\tdxhy.cfg'

# return_hy_list = return_hy_def(incon_dat_path, 'TDXNHY')
# init_hy_transmission_graph(test_graph, return_hy_list)
# append_hy_graph_index(tdxzs_cfg_path, test_graph)
# init_stock_transmission_graph(tdxhy_cfg_path, test_graph)
follow_assets = {'000939': ['user_a1', 'user_a2'],
                 '600291': [],
                 '600816': ['user_a1']}
macro_rrr_path = 'E:\\fintech_data\\tushare\\macro_economy\\rrr.csv'

import pandas as pd

rrr_df = pd.read_csv(macro_rrr_path)
rrr_df['date'] = pd.to_datetime(rrr_df['date'])
rrr_df.set_index('date', inplace=True)
rrr_df.sort_index(inplace=True)

import get_hist_k_day

big_volati = get_hist_k_day.ret_big_Volatility(
    get_hist_k_day.get_tdx_hist_k_data('E:\\fintech_data\\tdx\\k_line\\K_day\\42#T007.csv'))

dates = pd.date_range(start='19850101', end='20180517')
big_event = pd.DataFrame(index=dates)
big_event['rrr'] = rrr_df['changed']
big_event['T007'] = big_volati
big_event = big_event.dropna(how='all')

follow_user = 'user_a1'
for day in dates.tolist():
    if day in big_event.index.tolist():
        if pd.notnull(big_event.loc[day, 'rrr']):
            # get the leaf node
            data = test_graph.data(
                "match(n:event)-[r:Transmission*1..7]->(relateNode) where n.event_type='rrr' and size((relateNode)-[]->())=0 return relateNode")
            event = 'rrr'

        if pd.notnull(big_event.loc[day, 'T007']):
            event_node = test_graph.find('event', property_key='event_type', property_value='T007')
            data = test_graph.data(
                "match(n:event)-[r:Transmission*1..7]->(relateNode) where n.event_type='T007' and size((relateNode)-[]->())=0 return relateNode")
            event = 'T007'

        transmited_assets = [x['relateNode']['tdx_stock_code'] for x in data]
        transmited_follow_asset = set(transmited_assets) & set(follow_assets.keys())
        for tra in transmited_follow_asset:
            if follow_user in follow_assets[tra]:
                print(day, follow_user, '关注的', tra, '因', event, '大波动造成影响！')
    else:
        # print(day, follow_user, '关注的资产今天没有影响')
        pass
