# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: merger_stock_data.py
    @time: 2017/11/17 9:23
--------------------------------
"""
import sys
import os
import pandas as pd
import numpy as np
import xlrd

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('merger_stock_data.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('merger_stock_data.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class merger_stock_data(object):
    def __init__(self):
        pass


if __name__ == '__main__':
    df = pd.DataFrame([])
    for file in os.listdir('C:\\Users\\Administrator\\Desktop\\stock_data'):
    #file = 'zxc.xlsx'
        try:
            data = xlrd.open_workbook('C:\\Users\\Administrator\\Desktop\\stock_data\\%s' %file)

            for i in range(len(data.sheet_names())):
                table = data.sheet_by_index(i)
                stock_code = table.cell(0, 0).value
                stock_name = table.cell(0, 1).value
                df0 = pd.read_excel('C:\\Users\\Administrator\\Desktop\\stock_data\\%s' %file, sheet_name=i, skiprows=[0, ])

                df0[u'日期'] = df0[u'日期'].astype(np.str)
                df0['stock_code'] = str(stock_code).replace("'", "")
                df0['stock_name'] = stock_name
                df0['key'] = df0['stock_code'] + '/' + df0[u'日期']
                df0 = df0.drop([u'成交笔数', 'MA1', 'MA2', 'MA3', 'MA4', 'MA5'
                                   , 'MA6'], axis=1)
                print df0.head(3)
                df = df.append(df0, ignore_index=True)
        except:
            pass
    df.columns = ['value_date','opening_price','high_price','low_price','closing_price','trading_volume','turnover','stock_code','stock_name','crawler_key']
    df.to_csv('C:\\Users\\Administrator\\Desktop\\stock_data\\merged-%s.csv' %'all', encoding='utf8')