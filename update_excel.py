# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: update_excel.py
    @time: 2017/11/15 18:51
--------------------------------
"""
import sys
import os

import datetime
import pandas as pd
import numpy as np
import time
from contextlib import closing
import pymysql

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('update_excel.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('update_excel.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class update_excel(object):
    def __init__(self):
        pass

    def get_data(self, code, date1, date2):
        date1 = date1.strftime('%Y-%m-%d')
        date2 = date2.strftime('%Y-%m-%d')
        sql = 'SELECT `crawler_key`, `fund_code`, `value_date`,`accumulative_net_value` FROM `eastmoney_daily_data` WHERE `fund_code` = %s AND (`value_date` BETWEEN "%s" AND "%s")' %(code,date1,date2)
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        print df.head(5)
        return df

    def main(self):
        summary_listing = pd.read_excel('summary_listing.xlsx', skiprows=[0, ], dtype=np.str)
        summary_listing = summary_listing.replace('nan', '')
        code_ser = summary_listing[u'基金代码']

        for code in code_ser:
            start_time = time.time()

            print code, '===>'
            if not code:
                print "pass NaN"
                continue

            row = summary_listing[summary_listing[u'基金代码'] == code].index
            print u'数据在总表中的第%s列' % list(row)

            date1 = datetime.datetime(1900, 1, 1)
            date2 = datetime.datetime(2017, 10, 31)
            summary_listing = self.mul_cal(summary_listing, row, code, date1, date2, u'年化收益',
                                           [u'历史年化收益率',u'历史最大回撤',u'历史年化收益回撤比'],
                                           [u'年化收益',u'回撤',u'收益回撤比'])

            date1 = datetime.datetime(2017, 1, 1)
            date2 = datetime.datetime(2017, 10, 31)
            summary_listing = self.mul_cal(summary_listing, row, code, date1, date2, u'年化收益',
                                           [u'今年年化收益',u'今年回撤',u'今年收益回撤比'],
                                           [u'年化收益',u'回撤',u'收益回撤比'])


            date1 = datetime.datetime(2016, 10, 1)
            date2 = datetime.datetime(2016, 10, 31)
            summary_listing = self.mul_cal(summary_listing, row, code, date1, date2, u'收益',
                                           [u'2016年10月收益',],[u'收益',])

            date1 = datetime.datetime(2016, 11, 1)
            date2 = datetime.datetime(2016, 11, 30)
            summary_listing = self.mul_cal(summary_listing, row, code, date1, date2, u'收益',
                                           [u'2016年11月收益',],[u'收益',])

            date1 = datetime.datetime(2016, 12, 1)
            date2 = datetime.datetime(2016, 12, 31)
            summary_listing = self.mul_cal(summary_listing, row, code, date1, date2, u'收益',
                                           [u'2016年12月收益',],[u'收益',])

            print "耗时:", time.time() - start_time

        summary_listing.to_excel('test.xlsx', index=None)

    def mul_cal(self, summary_listing, row, fund_code, date1, date2, earnings_type, needed_data, co_col):
        ser = self.simple_cal(fund_code, date1, date2, earnings_type)
        if ser is not None:
            for i in range(len(needed_data)):
                summary_listing.loc[row, needed_data[i]] = ser[co_col[i]]
        return summary_listing

    def simple_cal(self, fund_code, date1, date2, earnings_type):
        #print 'mark00'
        date_col = u'value_date'
        value_col = u'accumulative_net_value'
        target_col = u'retracement'
        df = self.get_data(fund_code, date1, date2) # 从数据库读取数据
        df.loc[:,date_col] = pd.to_datetime(df.loc[:,date_col])

        # 截取所需时间内的数据,数据取到小于date1的最后一个
        serx = df[date_col][df[date_col].between(datetime.datetime(1800,1,1), date1 - datetime.timedelta(1))]
        if not serx.empty:
            serx = serx.sort_values()
            date1 = serx.iat[-1]

        df = df[df[date_col].between(date1, date2)]
        # 修正日期
        df = df.sort_values([date_col, ]) # 按时间排序
        if df.empty:
            return None
        date1 = df[date_col].iat[0]
        date2 = df[date_col].iat[-1]

        func_max = lambda date: max(df[value_col][df[date_col] <= date])
        df.index = range(df.shape[0])


        df['accumulative_net_value'] = df['accumulative_net_value'].astype(np.float16)
        for i in range(1, df.shape[0]):
            #print 'mark1'
            value0 = df.loc[i, value_col]
            date0 = df.loc[i, date_col]
            df.loc[i, 'max'] = func_max(date0)
            if value0 < func_max(date0):
                df.loc[i, target_col] = (value0 / func_max(date0)) - 1
            else:
                df.loc[i, target_col] = 0

        df.to_csv('caculated_data.csv', encoding='utf_8_sig')

        if earnings_type == u'年化收益':
            product_yield = (df[value_col].iat[-1]/df[value_col].iat[0]-1) * 365.0 / (df[date_col].iat[-1]-df[date_col].iat[0]).days  # 最后一个除以第一个，减1
        elif earnings_type == u'收益':
            product_yield = df[value_col].iat[-1] / df[value_col].iat[0] - 1 # 最后一个除以第一个，减1
        else:
            product_yield = None
        retracement = min(df[target_col].dropna())
        earnings_retracement_ratio = product_yield / (-retracement)


        print date1,date2,'product_yield,retracement,earnings_retracement_ratio:',product_yield,retracement, earnings_retracement_ratio

        # df.to_csv('cal-' + file_name, encoding='utf8')
        return pd.Series([df['fund_code'].iat[0], product_yield, retracement, earnings_retracement_ratio], index=[u'基金代号', earnings_type, u'回撤', u'收益回撤比'])

if __name__ == '__main__':
    update_excel = update_excel()
    update_excel.main()