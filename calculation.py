# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: calculation.py
    @time: 2017/11/28 13:39
--------------------------------
"""
import sys
import os

import datetime
import pandas as pd
import numpy as np

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('calculation.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('calculation.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class calculation(object):
    def __init__(self):
        pass

    def earnings_cal(self, fund_code, df, **kwarg):
        """

        :param fund_code: 净值的数列
        :param df:
        :param kwarg:
        :return:
        """
        if df.empty:
            print 'earnings_cal=>', fund_code, u'数据为空'
            return None

        if kwarg:
            date_col = kwarg['date_col']
            value_col = kwarg['value_col']
        else:
            date_col, value_col = df.columns.tolist()[0,1]

        date1 = kwarg['date1'] if 'date1' in kwarg else min(pd.to_datetime(df.loc[:, date_col]))
        date2 = kwarg['date2'] if 'date2' in kwarg else max(pd.to_datetime(df.loc[:, date_col]))
        if date1 == date2:
            print u'earnings_cal=>输入日期间隔为0'
            return None

        print u"正在准备计算%s的从%s到%s的数据" % (fund_code, date1, date2)
        #print date_col, value_col, date1, date2
        target_col = u'retracement'

        df = df.dropna()  # 剔除目标字段中的空值行
        df = df[df[value_col].astype(np.str) != ''].copy() # 剔除目标字段中的空白字符
        df.loc[:, date_col] = pd.to_datetime(df.loc[:, date_col]) # 将日期列的字符串转为日期

        # 截取所需时间内的数据,数据取到小于date1的最后一个
        if 'data_type' in kwarg and kwarg['data_type'] == 'fund':
            serx = df[date_col][df[date_col].between(datetime.datetime(1800, 1, 1), date1 - datetime.timedelta(1))]
            if not serx.empty:
                serx = serx.sort_values()
                date1 = serx.iat[-1]
        # 截取规定时间段内的数据
        df = df[df[date_col].between(date1, date2)].copy()
        if df.empty:
            print u'earnings_cal =>时间段内数据为空'
            return None


        df = df.sort_values([date_col, ])  # 按时间排序
        func_max = lambda date: max(df[value_col][df[date_col] <= date]) # 截取日期小于某一天的所有数据中的最大值

        df = df.reset_index(drop=True)
        df[value_col] = df[value_col].astype(np.float) # 更改目标列的数据格式

        df['max'] = df[value_col].expanding(min_periods=1).max()
        df[target_col] = (df[value_col] / df['max'] - 1).copy()
        #print df

        annual_yield = (df[value_col].iloc[-1] / df[value_col].iloc[0] - 1) * 365.0 / (df[date_col].iloc[-1] - df[date_col].iloc[0]).days
        # print date1,date2,df[value_col]
        product_yield = df[value_col].iloc[-1] / df[value_col].iloc[0] - 1  # 最后一个除以第一个，减1
        retracement = min(df[target_col].dropna())
        if retracement:
            annual_earnings_retracement_ratio = annual_yield / (-retracement)
            earnings_retracement_ratio = product_yield / (-retracement)
        else:
            annual_earnings_retracement_ratio = None
            earnings_retracement_ratio = None

        return pd.Series([fund_code, product_yield, retracement, earnings_retracement_ratio, annual_yield, annual_earnings_retracement_ratio],
                         index=[u'基金代号', u'收益率', u'最大回撤', u'收益回撤比', u'年化收益率',u'年化收益回撤比'])

if __name__ == '__main__':
    calculation = calculation()