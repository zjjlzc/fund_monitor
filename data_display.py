# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_display.py
    @time: 2017/11/17 15:54
--------------------------------
"""
import sys
import os
import pandas as pd
import numpy as np
from contextlib import closing
import pymysql
import time

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('data_display.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('data_display.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

needed_code = \
"""
002803
519690
110022
180012
001542
000742
519066
340007
003095
000457
519772
377010
519095
163116
310358
020026
001410
001071
710001
000063
001975
002259
169105
001371
001878
002121
002214
004477
377240
370024
000755
003624
310388
165520
206007
160222
376510
090003
398031
003940
001112
000619
003940
165312
110023
240020
000884
000878
519195
001712
002851
162607
001179
000251
001740
001659
040008
121005
580003
257070
001956
165523
375010
580008
000522
003769
519606
001703
""".split('\n')



class data_display(object):
    def __init__(self):
        pass

    def get_data(self ,date_str1, date_str2):
        sql = """
        SELECT `fund_code`, `value_date`, `estimate_net_value`, `net_asset_value`, `estimate_daily_growth_rate`, `daily_growth_rate`
        FROM `eastmoney_daily_data`
        WHERE `value_date` BETWEEN str_to_date("%s", "%s") AND str_to_date("%s", "%s")
        """ %(date_str1,"%Y-%m-%d", date_str2,"%Y-%m-%d")
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)

        return df

    def one_fund(self, df0):
        df0.index = df0['value_date'].astype(np.str)

        fund_code = df0['fund_code'].iat[0]
        ser = pd.Series([fund_code,'',''], index=['基金代码', '基金简称', '基金类型'])

        d = {
            'estimate_net_value':'估算净值',
            'net_asset_value':'净值',
            'estimate_daily_growth_rate':'估算日收益率',
            'daily_growth_rate': '日收益率'
        }
        for key in d:
            ser0 = df0[key]
            if len(''.join(ser0.fillna('').tolist())):
                ser0.index = [s + d[key] for s in ser0.index]
                ser = ser.append(ser0)

        #ser = ser.sort_index(ascending=False)
        print ser
        return ser


    def main(self):
        start_time = time.time()
        df_all = self.get_data('2017-11-1','2017-11-30')
        code_list = df_all['fund_code'].drop_duplicates().tolist()
        df = pd.DataFrame([])
        for code in code_list:
            df0 = df_all[df_all['fund_code']==code]
            ser = self.one_fund(df0)
            df = df.append(ser, ignore_index=True)
        title = df.columns.tolist()
        title.sort(reverse=True)
        df1 = df[df['基金代码'].isin(needed_code) == True]
        df2 = df[df['基金代码'].isin(needed_code) == False]
        df = pd.DataFrame([]).append(df1,ignore_index=True).append(df2,ignore_index=True)
        df.to_csv('merged_value_data.csv', encoding='gbk', columns=title, index=None)
        #time.sleep(3)
        print "耗时：", time.time()-start_time



if __name__ == '__main__':
    data_display = data_display()
    data_display.main()