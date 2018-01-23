# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: mock_trading.py
    @time: 2017/12/6 15:56
--------------------------------
"""
import sys
import os

import datetime
import pandas as pd
import numpy as np

import api51
api51 = api51.api51()
from contextlib import closing
import pymysql
import copy


sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('mock_trading.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('mock_trading.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

user_key = '1923eae2a3054d4c92a7eb74d7f65396'

# d = {
#     'prod_code':'000001.SS',
#     'candle_mode':'0',
#     'data_count':'1000',
#     'get_type':'offset',
#     'search_direction':'1',
#     'candle_period':'6',
# }
# json_data = api51.connect(user_key, d)
# df = pd.DataFrame(json_data['data']['candle']['000001.SS'])
# date_ser = df[0].apply(lambda num:datetime.datetime.strptime(str(num),'%Y%m%d'))

initial_capital = 100000

class mock_trading(object):

    def __init__(self):
        pass

    def get_stock_data(self, stock_code, date1, date2):
        # 获取股票类型
        stock_code0 = copy.deepcopy(stock_code)
        sql = 'SELECT `stock_code`, `stock_type` FROM `stock_belonging` WHERE `stock_code`=\'%s\'' % stock_code
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        stock_type = df['stock_type'].iat[0]
        stock_code = str(stock_code) + '.' + stock_type

        d = {
            'candle_mode': '1',
            'candle_period': '6',
            'get_type': 'range',
            'prod_code': stock_code,
            'search_direction': '1',
            'start_date':date1.strftime('%Y%m%d'),
            'end_date':date2.strftime('%Y%m%d'),
        }
        json_data = api51.connect(user_key, d)[u'data'][u'candle']

        df = pd.DataFrame(json_data[stock_code], columns=json_data[u'fields'])
        df['stock_code'] = stock_code
        df['min_time'] = pd.to_datetime(df['min_time'], format='%Y%m%d')
        if df.empty:
            raise Exception(u'不存在股票%s' %stock_code)
        return df

    def mock_trading(self):
        trading_detail = pd.read_excel('trading_detail.xls')

        stock_list = trading_detail[u'股票代号'].drop_duplicates().tolist()
        date_ser = pd.to_datetime(trading_detail[u'日期'])

        stock_data = pd.DataFrame([])
        for stock_code in stock_list:
            stock_data = stock_data.append(self.get_stock_data(stock_code, date_ser.min(), date_ser.max()))
        print stock_data

        res = pd.DataFrame([], index=date_ser.tolist())
        for date in res.index:
            if date == date_ser.min():
                cur_capital = initial_capital
            res[date, u'期初资金'] = cur_capital
            day_detail = trading_detail[trading_detail[u'日期'] == date]

            # 验证价格合理性
            # for t in day_detail.iterrow:









if __name__ == '__main__':
    mock_trading = mock_trading()
    mock_trading.mock_trading()