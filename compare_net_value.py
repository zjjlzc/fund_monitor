# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: compare_net_value.py
    @time: 2017/11/20 11:04
--------------------------------
"""
import sys
import os
import datetime
import traceback

import pandas as pd
import numpy as np
import pymysql
from contextlib import closing
import cal_fitting_net_value
cal_fitting_net_value = cal_fitting_net_value.cal_fitting_net_value()
import net_value_cal_display
net_value_cal_display = net_value_cal_display.net_value_cal_display()
import requests_manager
requests_manager = requests_manager.requests_manager()
import json
import time

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('compare_net_value.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('compare_net_value.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

user_key = '83E30DFE2C71435EA71000DCAFC4AD84'

class compare_net_value(object):
    def __init__(self):
        pass


    def get_net_value(self, fund_code, date_str1, date_str2):
        sql = "SELECT `fund_code`, `value_date`, `net_asset_value` FROM `eastmoney_daily_data` " \
              "WHERE `fund_code` = '%s' AND `value_date` BETWEEN '%s' AND '%s'" %(fund_code, date_str1, date_str2)

        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn, index_col='value_date')
        df.index = [date0.strftime('%Y-%m-%d') for date0 in df.index]
        df = df.drop(df[df['net_asset_value'] == ''].index.tolist(), axis=0)
        return df


    def simple_cal(self, ser, earnings_type):
        if ser is None:
            return None
        df = pd.DataFrame(ser)
        df['max'] = ''
        df['target_col'] = ''
        print df.shape
        func_max = lambda date: max(df.iloc[:,0][df.index <= date])
        for i in range(1, df.shape[0]):
            value0 = df.iloc[i, 0]
            date0 = df.index[i]
            df.iloc[i, 1] = func_max(date0)
            if value0 < func_max(date0):
                df.iloc[i, 2] = (value0 / func_max(date0)) - 1
            else:
                df.iloc[i, 2] = 0

        #print df.shape
        #df = df.fillna('')

        if earnings_type == u'年化收益率':
            product_yield = (df.iloc[:,0].iat[-1] / df.iloc[:,0].iat[0] - 1) * 365.0 / (datetime.datetime.strptime(df.index[-1],'%Y-%m-%d') - datetime.datetime.strptime(df.index[0],'%Y-%m-%d')).days  # 最后一个除以第一个，减1
        elif earnings_type == u'收益率':
            product_yield = df.iloc[:,0].iat[-1] / df.iloc[:,0].iat[0] - 1  # 最后一个除以第一个，减1
        else:
            product_yield = None
        retracement = min(df.iloc[:, 2].dropna())
        earnings_retracement_ratio = product_yield / (-retracement)

        return pd.Series([product_yield, retracement, earnings_retracement_ratio], index=[earnings_type, u'回撤', u'收益回撤比'])


    def data_display(self):
        with open(r'important_fund.txt', 'r') as f:
            fund_code_list = f.read().split('\n')
        #fund_code_list = ['001542',]
        date_l = [
            # ['2016-09-30', '2016-10-20', '2017-04-19'],
            # ['2017-03-31', '2017-04-20', '2017-08-20'],
            # ['2017-06-30', '2017-08-21', '2017-10-20'],
            ['2017-09-30', '2017-10-21', '2020-01-01'],
        ]
        for fund_code in fund_code_list:
            print u"正在计算基金：", fund_code
            start_time = time.time()
            try:
                web_net_value = self.get_net_value(fund_code, date_l[0][1], date_l[-1][-1])
                #print web_net_value.index.tolist()
                web_net_value.to_csv('web_net_value.csv')
                cal_net_value = cal_fitting_net_value.combine_net_value(fund_code, date_l)
                #print cal_net_value.index.tolist()
                cal_net_value.to_csv('cal_net_value.csv')
                df = web_net_value.join(cal_net_value)

                df = df.dropna()
                #for i in range(df.shape[0]):
                #    print df.iloc[i,:].tolist()
                df.loc[:, 'cal_net_asset_value'] = df.loc[:, 'net_asset_value'].apply(lambda x:float(x)/float(df['net_asset_value'].iat[0]))

                # df.to_csv(fund_code+'.csv')
                # pd.Series(['fitting_net_value',]).to_csv(fund_code+'.csv', mode='a')
                # pd.DataFrame(self.simple_cal(df['fitting_net_value'],u'收益率')).to_csv(fund_code+'.csv', mode='a')
                # pd.DataFrame(self.simple_cal(df['fitting_net_value'], u'年化收益率')).to_csv(fund_code+'.csv', mode='a')
                # pd.Series(['cal_net_asset_value', ]).to_csv(fund_code+'.csv', mode='a')
                # pd.DataFrame(self.simple_cal(df['cal_net_asset_value'], u'收益率')).to_csv(fund_code + '.csv', mode='a')
                # pd.DataFrame(self.simple_cal(df['cal_net_asset_value'], u'年化收益率')).to_csv(fund_code + '.csv', mode='a')
                df.index.name = fund_code
                dfx = df.T.copy()
                #dfx = dfx.join(pd.Series(['fitting_net_value',],name = 0))
                ser = self.simple_cal(df['fitting_net_value'],u'收益率')
                ser.name = fund_code + u'拟合净值收益率'
                ser.index = ['fund_code', 'net_asset_value', 'fitting_net_value']
                dfx = dfx.join(ser)

                ser = self.simple_cal(df['fitting_net_value'],u'年化收益率')
                ser.name = fund_code + u'拟合净值年化收益率'
                ser.index = ['fund_code', 'net_asset_value','fitting_net_value']
                dfx = dfx.join(ser)

                ser = self.simple_cal(df['cal_net_asset_value'],u'收益率')
                ser.name = fund_code + u'折算净值收益率'
                ser.index = ['fund_code', 'net_asset_value', 'fitting_net_value']
                dfx = dfx.join(ser)

                ser = self.simple_cal(df['cal_net_asset_value'],u'年化收益率')
                ser.name = fund_code + u'折算净值年化收益率'
                ser.index = ['fund_code', 'net_asset_value', 'fitting_net_value']
                dfx = dfx.join(ser)

                dfx.reindex(reversed(dfx.columns), axis=1).to_csv('compare_net_value.csv', mode='a')

                print u"耗时：", time.time() - start_time
            except:
                print traceback.format_exc()

        pd.read_csv('compare_net_value.csv').T.to_csv('compare_net_value.csv')



if __name__ == '__main__':
    compare_net_value = compare_net_value()
    compare_net_value.data_display()