# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: calculation.py
    @time: 2017/11/16 17:16
--------------------------------
"""
import sys
import os
import pandas as pd
from contextlib import closing
import pymysql
import xlrd
import numpy as np
import re

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('calculation.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('calculation.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class calculation(object):
    def __init__(self):
        self.date_list = []

    def cal1(self):
        file0 = 'ranking.xlsx'
        data = xlrd.open_workbook(file0)
        df = pd.DataFrame([])
        for sheet_name in data.sheet_names():
            df0 = pd.read_excel(file0, sheet_name=sheet_name)
            print np.array(df0)

    def cal_stock(self, stock_code, date_str1, date_str2):
        df = self.get_stock_data(stock_code, date_str1, date_str2)
        df = df.sort_values(['value_date',])
        df.index = range(df.shape[0])
        for i in range(df.shape[0]):
            df.loc[i, 'cal_value'] = float(df.loc[i, 'closing_price'])/float(df.loc[0, 'closing_price'])
        return df


    def get_stock_data(self, stock_code, date_str1, date_str2):
        sql = """
        SELECT `stock_code`,`closing_price`,`value_date`
        FROM `stock_daily_data`
        WHERE `stock_code`="%s" AND `value_date` BETWEEN str_to_date("%s", "%s") AND str_to_date("%s", "%s")
        ORDER BY `value_date`
        #GROUP BY `stock_code`
        """ % (stock_code, date_str1,"%Y-%m-%d", date_str2,"%Y-%m-%d")
        #print sql
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df0 = pd.read_sql(sql, conn)

        if df0.empty:
            print stock_code, u"无股票数据"
            value0 = raw_input(u"输入代替%s缺失值的收盘价:" %stock_code)
            return pd.DataFrame([[stock_code, value0, date_str] for date_str in self.date_list], columns=['stock_code','closing_price','value_date'])
        else:
            self.date_list = df0['value_date'].tolist()
            df.fillna(method='ffill')
            return df0

    def get_fund_holdings_data(self, fund_code, cut_off_date):

        sql = """
        SELECT `fund_code`, `cut_off_date`, `stock_code`, `net_value_ratio`
        FROM `fund_holdings`
        WHERE fund_code = "%s" AND `cut_off_date` = "%s"
        ORDER BY `net_value_ratio` DESC
        LIMIT 10
        """ % (fund_code, cut_off_date)
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)

        # 将前十的数据比例折算成综合100%
        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda s:float(s.replace('%',''))/100.0)
        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda i:i * (1/sum(df['net_value_ratio'])))

        # 将港股标志去掉
        df['stock_code'] = df['stock_code'].apply(lambda s:re.search(r'\d+',s).group() if re.search(r'\d+\.HK',s) else s)

        return df


if __name__ == '__main__':
    calculation = calculation()
    fund_code = '161725'
    cut_off_date = '2016-12-31'
    date_str1 = '2017-6-31'
    date_str2 = '2018-12-31'
    df = calculation.get_fund_holdings_data(fund_code, cut_off_date)
    if df.empty:
        raise Exception(u'%s没有从%s到%s的持仓数据' %(fund_code, date_str1, date_str2))

    ser = None
    for i in xrange(df.shape[0]):
        stock_code = df.loc[i, 'stock_code']
        value_ratio = df.loc[i, 'net_value_ratio']
        print u'计算股票%s' %stock_code
        res = calculation.cal_stock(stock_code, date_str1, date_str2)
        #print res
        res.index = res['value_date']
        if ser is None:
            ser = res['cal_value'] * value_ratio /100 # 个股每日的折算净值
        else:
            ser = ser + res['cal_value'] * value_ratio /100
    ser.to_csv('%s(%s)_(%s)' %(fund_code, date_str1, date_str2))

    #print calculation.cal_stock('600519')
