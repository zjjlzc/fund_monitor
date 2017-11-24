# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: fund_holdings_display.py
    @time: 2017/11/18 11:37
--------------------------------
"""
import sys
import os
import traceback

import pandas as pd
import numpy as np
from contextlib import closing
import pymysql
import datetime
import xlrd
import xlwt
from xlutils.copy import copy
import re

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('fund_holdings_display.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('fund_holdings_display.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class fund_holdings_display(object):
    def __init__(self):
        sql = """
        SELECT `fund_code`, `fund_name`
        FROM `fund_info`
        """
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            self.fund_info = pd.read_sql(sql, conn)
            self.fund_info.index = self.fund_info['fund_code']
    
    def get_data(self, fund_code):
        sql = """
        SELECT `fund_code`, `cut_off_date`, `stock_code`, `stock_name`, `net_value_ratio`, `share_holding`
        FROM `fund_holdings`
        WHERE `fund_code` = %s
        """ % fund_code
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        #print df['net_value_ratio'].tolist()
        #print df
        df['net_value_ratio'] = df['net_value_ratio'].apply(lambda s: float(re.search(r'\d+\.\d*', s).group())/100 if re.search(r'\d+\.\d*', s) else 0.0)
        #print df
        df = df.sort_values(['net_value_ratio'], ascending=False)
        df.index = df['stock_code'].tolist()
        return df
        
    def data_classify(self, df):
        cut_off_days = df['cut_off_date'].drop_duplicates().tolist()
        d = {}
        for date0 in cut_off_days:
            df0 = df[df['cut_off_date']==date0].head(10)
            key = datetime.datetime.strptime(date0, "%Y-%m-%d")
            d[key] = df0
        return d

    def df_status(self, date_d):
        key_list = sorted(date_d.keys(), reverse=True)
        for i in range(len(key_list) - 1):
            df1 = date_d[key_list[i]]
            df2 = date_d[key_list[i + 1]]

            df1['change'] = ''
            new_stock = []
            for stock_code in df1['stock_code'].tolist():
                if stock_code not in df2['stock_code'].tolist():
                    df1.loc[stock_code, 'change'] = float(df1.loc[stock_code, 'net_value_ratio'])
                    new_stock.append(df1.loc[stock_code,'stock_name'])
                else:
                    df1.loc[stock_code, 'change'] = float(df1.loc[stock_code, 'net_value_ratio']) - float(df2.loc[stock_code, 'net_value_ratio'])
            missing_stock = []
            for stock_code in df2['stock_code'].tolist():
                if stock_code not in df1['stock_code'].tolist():
                    missing_stock.append(df2.loc[stock_code,'stock_name'])

            df1.loc['合计', ['net_value_ratio', 'share_holding']] = [sum(df1['net_value_ratio'].astype(np.float)),
                                                                      sum(df1['share_holding'].astype(np.float))]
            df1.loc['注1:', 'stock_name'] = "较上一期新进:%s" % '，'.join(new_stock)
            df1.loc['注2:', 'stock_name'] = "较上一期缺少:%s" % '，'.join(missing_stock)

            df1 = df1.fillna('')
            df1['net_value_ratio'] = df1['net_value_ratio'].apply(lambda num: ('%s%s' % (round(float(num)*100, 4), '%')) if num else '')
            df1['change'] = df1['change'].apply(lambda num: ('%s%s' % (round(float(num)*100, 4), '%')) if num else '')
            df1 = df1.rename({'stock_name':'股票名称','net_value_ratio':'持股占比','share_holding':'持股量','change':'占比变化'}, axis=1)
            #df.index.name = '股票代码'
            date_d[key_list[i]] = df1
        return date_d

    def data_display(self, date_d):
        key_list = sorted(date_d.keys(), reverse=True)

        fund_info = date_d[key_list[0]].iloc[0,:]
        subj = ''
        fund_code = fund_info['fund_code']
        #print len(key_list)
        if os.path.exists('temp_data.xls'):
            os.remove('temp_data.xls')
        workbook = xlwt.Workbook()
        workbook.add_sheet('0')
        workbook.save('temp_data.xls')

        oldWb = xlrd.open_workbook(r'temp_data.xls')
        newWb = copy(oldWb)
        newWs = newWb.get_sheet(0)
        newWs.write(0, 0, u'主题')
        newWs.write(0, 1, u'基金代号')
        newWs.write(0, 2, u'基金名称')
        newWs.write(1, 0, subj)
        newWs.write(1, 1, fund_code)
        newWs.write(1, 2, self.fund_info.loc[fund_code,'fund_name'])
        newWb.save(r'temp_data.xls')

        for i in range(len(key_list))[:5]:
            #print i
            key = key_list[i]
            df = date_d[key]
            df = df.drop(['fund_code', 'cut_off_date', 'stock_code'], axis=1)
            df.to_excel('temp.xlsx')

            # 以下代码极度丑陋
            workbook = xlrd.open_workbook(r'temp.xlsx')
            sheet = workbook.sheet_by_index(0)
            nrows = sheet.nrows
            row_list = []
            for i in range(nrows):
                row_list.append(sheet.row_values(i))

            oldWb = xlrd.open_workbook(r'temp_data.xls')
            sheet = oldWb.sheet_by_index(0)
            nrows = sheet.nrows
            ncols = sheet.ncols
            #print nrows,type(nrows)

            newWb = copy(oldWb)
            newWs = newWb.get_sheet(0)
            newWs.write(0, ncols, key.strftime("%Y-%m-%d"))
            for r in range(len(row_list)):
                row = row_list[r]
                for c in range(len(row)):
                    newWs.write(r+1, c+ncols, row[c])

            newWb.save(r'temp_data.xls')

    def display_all(self):
        try:
            with open('important_fund.txt', 'r') as f:
                code_list = f.read().split('\n')

            # sql = """
            # SELECT DISTINCT `fund_code` FROM `eastmoney_daily_data`
            # """
            # with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            #     df0 = pd.read_sql(sql, conn)
            # for code in df0.iloc[:, 0].tolist():
            #     if code not in code_list:
            #         code_list.append(code)

            if os.path.exists('final_output.csv'):
                os.remove('final_output.csv')

            for fund_code in code_list:
                print fund_code
                df = fund_holdings_display.get_data(fund_code)
                date_d = fund_holdings_display.data_classify(df)
                date_d = fund_holdings_display.df_status(date_d)

                if len(date_d) < 1:
                    print 'pass', fund_code
                    continue
                fund_holdings_display.data_display(date_d)

                df_tmp = pd.read_excel(r'temp_data.xls')
                df_tmp.to_csv(r'final_output.csv', mode='a')
                pd.Series(['', ]).to_csv(r'final_output.csv', mode='a')
                with open('final_output.csv', 'r') as f:
                    text = re.sub('Unnamed: \d+', '', f.read())
                with open('final_output.csv', 'w') as f:
                    f.write(text)
        except:
            print traceback.format_exc()


    def comb_display(self):
        d = {
            '002803':0.2,
            '110022':0.4
        }
        #d = {key:float(d[key])/sum(d.values()) for key in d}

        fund_data = {}

        for fund_code in d:
            #print fund_code
            df = fund_holdings_display.get_data(fund_code) # 获取fund_code的数据
            date_d = fund_holdings_display.data_classify(df) # 将数据按截止日期分类
            fund_data[fund_code] = date_d # 存入字典

        # 统计共有的截止日期
        fund_date_set = reduce(lambda x,y:x & y,[set(fund_data[fund_code].keys()) for fund_code in fund_data])
        res = {}
        for date0 in fund_date_set:
            df = pd.DataFrame()
            # 将同一个截止日期的几个基金按给定权重求和
            for fund_code in fund_data:
                df0 = fund_data[fund_code][date0]
                param = d[fund_code]
                df0['net_value_ratio'] = df0['net_value_ratio'] * param
                # 怎么合并?
            res[date0] = df


if __name__ == '__main__':
    fund_holdings_display = fund_holdings_display()
    fund_holdings_display.display_all()

