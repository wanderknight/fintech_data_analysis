__author__ = 'wanderknight'
__time__ = '2018/5/8 19:48'
import pandas as pd
import datetime


def get_tdx_hist_k_data(path):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    return df


def returnize0(nds):
    """
    @summary Computes stepwise (usually daily) returns relative to 0, where
    0 implies no change in value.
    @return the array is revised in place
    """
    # if type(nds) == type(pd.DataFrame()):
    nds = (nds / nds.shift(1)) - 1.0
    nds = nds.fillna(0.0)
    return nds


def ret_big_Volatility(df):
    ret = returnize0(df['close'])
    ret_big = ret[(ret < (ret.mean() - ret.std() * 3))]  # (ret > (ret.mean() + ret.std() * 2.5)) |
    # print('ret_big_Volatility', ret_big.shape[0], '个点', '高波动占比：', ret_big.shape[0] / ret.shape[0])
    return ret_big


df1 = get_tdx_hist_k_data('E:\\fintech_data\\tdx\\k_line\\K_day\\SH#600000.csv')
df2 = get_tdx_hist_k_data('E:\\fintech_data\\tdx\\k_line\\K_day\\SH#999999.csv')

df_rel = pd.DataFrame()
df_rel['a'] = returnize0(df1['close'])
df_rel['b'] = returnize0(df2['close'])
df_rel['b'] = df_rel['b'].shift(-1)
# 原始序列的相关性
print('原始相关性', df_rel.shape[0], '个点,', 'corr:', df_rel.corr().iloc[0, 1])

df_big_rel = pd.DataFrame()
# 根据高波动的索引，
df_big_rel['a'] = ret_big_Volatility(df1)
df_big_rel['b'] = df_rel['b'].fillna(method='backfill').reindex(df_big_rel.index)
print('高波动相关', df_big_rel.shape[0], '个点,', 'corr:', df_big_rel.corr().iloc[0, 1])

# 去除高波动
df_rel_no_big = pd.DataFrame()
seta = set(df_rel.index) - set(df_big_rel.index)
df_rel_no_big = df_rel[df_rel.index.isin(seta)]
print('去除高波动的相关', df_rel_no_big.shape[0], '个点,', 'corr:', df_rel_no_big.corr().iloc[0, 1])

start_day = datetime.datetime(year=2000, month=1, day=1)
df_big_rel_recent = df_big_rel[df_big_rel.index > start_day]
print('近期高波动相关', df_big_rel_recent.shape[0], '个点,', 'corr:', df_big_rel_recent.corr().iloc[0, 1])

# 根据高波动的次数，随机采样比较高相关的占比，虽然有的相关比较高，但是波动幅度比较小也要忽略
count = 0
for i in range(1000):
    df_rel_tmp = df_rel[df_rel.index>start_day].dropna()
    df_sample = df_rel_tmp.sample(df_big_rel.shape[0])
    volati_high = False
    if df_sample[df_sample['a'] < (0.000500 - 0.014693 * 1)].shape[0] > df_big_rel.shape[0] / 5:
        volati_high = True

    if df_sample.corr().iloc[0, 1] > df_big_rel.corr().iloc[0, 1] and volati_high:
        count += 1
print(count / 1000)
