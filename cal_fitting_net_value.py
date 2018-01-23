# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: cal_fitting_net_value.py
    @time: 2017/11/16 17:16
--------------------------------
"""
import sys
import os

import datetime
import pandas as pd
from contextlib import closing
import pymysql
import xlrd
import numpy as np
import re
import time
import json
import copy

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import requests_manager
requests_manager = requests_manager.requests_manager()
import api51
api51 = api51.api51()


import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('cal_fitting_net_value.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('cal_fitting_net_value.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

user_key = '1923eae2a3054d4c92a7eb74d7f65396'

with open('stock_blacklist.txt','r') as f:
    stock_blacklist = f.read().split('\n')

d = {
    'prod_code':'000001.SS',
    'candle_mode':'0',
    'data_count':'1000',
    'get_type':'offset',
    'search_direction':'1',
    'candle_period':'6',
}
json_data = api51.connect(user_key, d)
df = pd.DataFrame(json_data['data']['candle']['000001.SS'])
date_ser = df[0].apply(lambda num:datetime.datetime.strptime(str(num),'%Y%m%d'))

class cal_fitting_net_value(object):
    def __init__(self):
        pass#self.date_list = []

    def cal1(self):
        file0 = 'ranking.xlsx'
        data = xlrd.open_workbook(file0)
        df = pd.DataFrame([])
        for sheet_name in data.sheet_names():
            df0 = pd.read_excel(file0, sheet_name=sheet_name)
            #print np.array(df0)

    # def get_stock_data(self, stock_code, date_str1, date_str2):
    #     # 修正股票代码
    #     #stock_code = stock_code.split('.')[0]
    #     if stock_code in stock_blacklist:
    #         print u"股票%s位于黑名单内" %stock_code
    #         return None
    #
    #     if re.search(r'[^\d]+', stock_code) and len(re.search(r'[^\d]+', stock_code).group()) == len(stock_code):
    #         stock_code = stock_code.split('.')[0]
    #     else:
    #         stock_code = re.search(r'\d+', stock_code).group()
    #     sql = """
    #     SELECT `stock_code`,`closing_price`,`value_date` FROM
    #     (
    #         (SELECT `stock_code`,`closing_price`,`value_date` AS `existed_value_date`
    #         FROM `stock_daily_data`
    #         WHERE `stock_code`="%s" AND `value_date` BETWEEN str_to_date("%s", "%s") AND str_to_date("%s", "%s")) L
    #
    #         RIGHT JOIN(
    #              SELECT `value_date` FROM `stock_daily_data`
    #              WHERE `value_date` BETWEEN str_to_date("%s", "%s") AND str_to_date("%s", "%s")
    #              GROUP BY `value_date`) R
    #         ON L.`existed_value_date` = R.`value_date`
    #     )
    #     ORDER BY `value_date`
    #     """ % (stock_code, date_str1,"%Y-%m-%d", date_str2,"%Y-%m-%d", date_str1,"%Y-%m-%d", date_str2,"%Y-%m-%d")
    #     #print sql
    #     with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
    #         df0 = pd.read_sql(sql, conn)
    #     #df0 = df0.drop(['existed_value_date', ], axis=1)
    #
    #     # 填充缺失数据
    #     # 若给定时间内最早的数据为空，则寻找更早的数据填充
    #     if df0.loc[0,:].isna().any() == True:
    #         #print u"时间范围内没有股票%s的数据，用以下数据填充" %stock_code
    #         sql = """
    #         SELECT `stock_code`,`closing_price`,`value_date`
    #         FROM `stock_daily_data`
    #         WHERE `stock_code`="%s" AND `value_date` < str_to_date("%s", "%s")
    #         ORDER BY `value_date` DESC
    #         LIMIT 1
    #         """ % (stock_code, date_str1, "%Y-%m-%d")
    #         #print sql
    #         with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
    #             dfx = pd.read_sql(sql, conn)
    #         #print df0
    #         if not dfx.empty:
    #             df0.iloc[0, 0:2] = dfx.iloc[0, 0:2]
    #         else:
    #             print u"不存在股票%s的任何历史数据数据，请输入数据填充值"
    #             value0 = raw_input(u"输入代替%s缺失值的收盘价:" % stock_code)
    #             return pd.DataFrame([[stock_code, value0, date_str] for date_str in self.date_list],
    #                                 columns=['stock_code', 'closing_price', 'value_date'])
    #
    #     df0 = df0.fillna(method='ffill')
    #     self.date_list = df0['value_date'].tolist()
    #     return df0

    def cal_stock(self, stock_code, date_str1, date_str2):
        df = self.get_stock_data(stock_code, date_str1, date_str2)
        if df is None:
            print stock_code, u'股票数据为空'
            return None
        df = df.sort_index()
        backup_index = df.index
        df.index = range(df.shape[0])
        #print df
        for i in range(df.shape[0]):
            df.loc[i, 'cal_value'] = float(df.loc[i, 'closing_price'])/float(df.loc[0, 'closing_price'])

        df.index = backup_index
        return df

    def get_stock_data(self, stock_code, date_str1, date_str2):
        if stock_code in stock_blacklist:
            print u"股票%s位于黑名单内" %stock_code
            return None

        print u"正在获取%s位于时间%s | %s内的数据" %(stock_code, date_str1, date_str2)
        date_range = date_ser[(date_ser>=datetime.datetime.strptime(date_str1,'%Y-%m-%d')) & (date_ser<=datetime.datetime.strptime(date_str2,'%Y-%m-%d'))]
        first_date = date_ser[date_ser>=datetime.datetime.strptime(date_str1,'%Y-%m-%d')].iloc[0]

        # 获取股票类型
        stock_code0 = copy.deepcopy(stock_code)
        sql = 'SELECT `stock_code`, `stock_type` FROM `stock_belonging` WHERE `stock_code`=\'%s\'' % stock_code
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        stock_type = df['stock_type'].iat[0]
        stock_code = stock_code + '.' + stock_type

        d = {
            'candle_mode': '1',
            'candle_period': '6',
            'get_type': 'range',
            'prod_code': stock_code,
            'search_direction': '1',
            'start_date':date_str1.replace('-',''),
            'end_date':date_str2.replace('-',''),
        }
        json_data = api51.connect(user_key, d)[u'data'][u'candle']

        df = pd.DataFrame(json_data[stock_code], columns=json_data[u'fields'])

        df = df.loc[:,['min_time','close_px']]
        df['stock_code'] = stock_code0

        df['min_time'] = df['min_time'].apply(lambda date:datetime.datetime.strptime(str(date), '%Y%m%d').strftime('%Y-%m-%d'))

        df = df.rename({'min_time':'value_date','close_px':'closing_price'}, axis=1)
        df.index = df['value_date']

        # 补全股票价格
        if df.empty or df['value_date'].iloc[0] != first_date.strftime('%Y-%m-%d'):
            if df.empty:
                print u'给定时间范围内，交易日的第一天为%s，没有股票数据' %first_date.strftime('%Y-%m-%d')
            else:
                print u'给定时间范围内，交易日的第一天为%s，获得的股票数据第一天为%s' %(first_date.strftime('%Y-%m-%d'), df['value_date'].iloc[0])
            d = {
                'candle_mode': '1',
                'candle_period': '6',
                'get_type': 'range',
                'prod_code': stock_code,
                'search_direction': '1',
                'end_date': date_str1.replace('-', ''),
            }
            json_data = api51.connect(user_key, d)
            df0 = pd.DataFrame(json_data['data']['candle'][stock_code], columns=json_data['data']['candle']['fields'])
            df.loc[first_date.strftime('%Y-%m-%d'), 'closing_price'] = df0['close_px'].iloc[-1]
            df.loc[first_date.strftime('%Y-%m-%d'), 'value_date'] = first_date.strftime('%Y-%m-%d')
            df.loc[first_date.strftime('%Y-%m-%d'), 'stock_code'] = stock_code.split('.')[0]
            df = df.sort_index()
        df = df.drop(['value_date',],axis=1)
        # print df
        df = df.reindex([date.strftime('%Y-%m-%d') for date in date_range]).fillna(method='ffill')
        # print df
        return df

    def get_fund_holdings_data(self, fund_code, cut_off_date):

        sql = """
        SELECT `fund_code`, `cut_off_date`, `stock_code`, 
               CONVERT(LEFT(`net_value_ratio`, INSTR(`net_value_ratio`,"%s")-1), DECIMAL(10,2)) AS `net_value_ratio`
        FROM `fund_holdings`
        WHERE fund_code = "%s" AND `cut_off_date` = "%s"
        ORDER BY `net_value_ratio` DESC
        LIMIT 10
        """ % ('%',fund_code, cut_off_date)
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        #print sql
        #print df
        # 将前十的数据比例折算成综合100%
        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda num:num/100.0)
        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda i:i * (1/sum(df['net_value_ratio'])))

        # 将港股标志去掉
        df['stock_code'] = df['stock_code'].apply(lambda s:re.search(r'\d+',s).group() if re.search(r'\d+\.HK',s) else s)
        #print df, sum(df['net_value_ratio'])
        return df

    def cal_net_value(self, fund_code, cut_off_date, date_str1, date_str2):
        df = self.get_fund_holdings_data(fund_code, cut_off_date)
        if df.empty or df is None:
            print u'%s has no fund_holdings data with cut_off_date: %s' % (fund_code, cut_off_date)
            return None
            #raise Exception(u'%s has no fund_holdings data with cut_off_date: %s' % (fund_code, cut_off_date))

        ser = None
        date_set = set()
        for i in xrange(df.shape[0]):
            stock_code = df.loc[i, 'stock_code']
            value_ratio = df.loc[i, 'net_value_ratio']
            #print 'value_ratio', value_ratio
            print u'计算股票%s' % stock_code
            res = self.cal_stock(stock_code, date_str1, date_str2)
            res = res.fillna(method = 'ffill')
            if res is None:
                return None
            # print res
            # print 'A'
            # print ser
            #print res
            if ser is None:
                ser = res['cal_value'] * value_ratio  # 根据基金持仓比例计算个股每日的折算净值
            else:
                ser = ser + res['cal_value'] * value_ratio
                # ser = ser.fillna(method='ffill')
            #print 'B'
            #print ser

        # print ser
        return ser
        #ser.to_csv('%s(cut_off_%s)(%s)_(%s).csv' % (fund_code, cut_off_date, date_str1, date_str2))

    def combine_net_value(self, fund_code, date_l):
        """
        :param date_l:
        # 截止日期， 股票取值起始日期， 股票取值截止日期
        date_l = [
            ['2016-9-30', '2016-10-20', '2017-04-19'],
            ['2017-3-31', '2017-04-20', '2017-08-20'],
            ['2017-6-30', '2017-08-21', '2017-10-20'],
            ['2017-9-30', '2017-10-21', '2020-01-01']
        ]
        """
        ser = pd.Series([]) # 初始净值数列
        yield_rate = 1 # 初始收益率为1
        for l in date_l:
            ser0 = self.cal_net_value(fund_code,l[0],l[1],l[2]) # 计算一段时间内拟合净值
            #print ser0
            if ser0 is None:
                print fund_code, u'没有此截止时间的拟合数据', l[0]
                continue
            ser= ser.append(ser0 * yield_rate) # 将新的拟合净值，乘以之前总净值数列的收益率后，合并入总净值数列
            yield_rate = ser.iloc[-1] / ser.iloc[0]  # 计算此时总净值数列的收益率
        ser.name = 'fitting_net_value'
        # print ser
        return ser

    # def single_fitting_net_value(self, stock_list, date_str1, date_str2):
    #     """
    #     stock_list = {
    #     '600519': 0.0857,
    #     '000568': 0.0819,
    #     '601318': 0.0810,
    #     '000858': 0.0782,
    #     '601398': 0.0780,
    #     '601601': 0.0778,
    #     '600036': 0.0734,
    #     '601288': 0.0720,
    #     '601688': 0.0421,
    #     '600809': 0.0356
    #     }
    #     返回一个拟合净值
    #
    #     """
    #
    #     ser = None
    #     for stock_code in stock_list:
    #         value_ratio = stock_list[stock_code]
    #         #print 'value_ratio', value_ratio
    #         print u'计算股票%s' % stock_code
    #         res = self.cal_stock(stock_code, date_str1, date_str2)
    #         res = res.fillna(method = 'ffill')
    #         if res is None:
    #             return None
    #
    #         if ser is None:
    #             ser = res['cal_value'] * value_ratio  # 根据基金持仓比例计算个股每日的折算净值
    #         else:
    #             ser = ser + res['cal_value'] * value_ratio
    #     return ser


if __name__ == '__main__':
    cal_fitting_net_value = cal_fitting_net_value()
    # code_list = ['001542','000457']
    # # 截止日期， 股票取值起始日期， 股票取值截止日期
    # date_l = [
    #     ['2016-09-30', '2016-10-20', '2017-04-19'],
    #     ['2017-03-31', '2017-04-20', '2017-08-20'],
    #     ['2017-06-30', '2017-08-21', '2017-10-20'],
    #     ['2017-09-30', '2017-10-21', '2020-01-01']
    # ]
    # for code in code_list:
    #     ser = cal_fitting_net_value.combine_net_value(code, date_l)
    #     ser.to_csv('%s.csv' % (code))
    # #print cal_fitting_net_value.cal_stock('600519')
    print cal_fitting_net_value.single_fitting_net_value('2017-10-21','2018-01-01')