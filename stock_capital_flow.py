# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: stock_capital_flow.py
    @time: 2017/12/13 9:28
--------------------------------
"""
import sys
import os
import pandas as pd
import numpy as np
import time
import chardet

sys.path.append(sys.prefix + "\\Lib\\MyWheels")

reload(sys)
sys.setdefaultencoding('utf-8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('stock_capital_flow.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('stock_capital_flow.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

import requests_manager
requests_manager = requests_manager.requests_manager()
import xls_manager
xls_manager = xls_manager.xls_manager()
print sys.getdefaultencoding()

class stock_capital_flow(object):

    def __init__(self):
        pass

    def stock_flow(self):
        token = '1942f5da9b46b069953c873404aad4b5'
        with open('stock_list.txt', 'r') as f:
            code_list = f.read().split('\n')
        print code_list
        data_dict = {}
        for stock_code in code_list:

            # http://ff.eastmoney.com//EM_CapitalFlowInterface/api/js?type=hff&rtntype=2&js=({data:[(x)]})&cb=var%20aff_data=&check=TMLBMSPROCR&acces_token=1942f5da9b46b069953c873404aad4b5&id=0000632&_=1513130772995
            data_str = ''
            # stock_type = 1
            while not data_str: # and stock_type < 4:
                url = "http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?type=HSGTHDSTA&token=%s&filter=(SCODE=%s%s%s)&st=HDDATE&sr=-1&p=1&ps=1000" % (token, '%27', stock_code, '%27')
                data_str = requests_manager.get_html(url)
                # stock_type = stock_type + 1
                time.sleep(1)

            df = pd.DataFrame(eval(data_str))
            if df.empty:
                print stock_code, u'内容为空'
                print 'url => ', url
            else:
                SNAME = df['SNAME'].iloc[0].decode('utf8')

                data_dict[SNAME] = df
                time.sleep(1)
                xls_manager.dfs_to_excel(data_dict, os.getcwd() + u'/股票资金流.xlsx')

            # df.to_excel('test.xlsx')
            # if data_str:
            #     data_row = data_str.split('\r\n')
            #     df = pd.DataFrame([s.split(',') for s in data_row], columns=[
            #                                                                 u'日期',
            #                                                                 u'收盘价',
            #                                                                 u'涨跌幅',
            #                                                                 u'主力净流入净额',
            #                                                                 u'主力净流入净占比',
            #                                                                 u'超大单净流入净额',
            #                                                                 u'超大单净流入净占比',
            #                                                                 u'大单净流入净额',
            #                                                                 u'大单净流入净占比',
            #                                                                 u'中单净流入净额',
            #                                                                 u'中单净流入净占比',
            #                                                                 u'小单净流入净额',
            #                                                                 u'小单净流入净占比'])
            #     data_dict[stock_code] = df
            # else:
            #     print stock_code, u'无数据'
            #     print data_str







if __name__ == '__main__':
    stock_capital_flow = stock_capital_flow()
    stock_capital_flow.stock_flow()