# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: report_output.py
    @time: 2017/12/6 11:23
--------------------------------
"""
import sys
import os

import datetime
import pandas as pd
import numpy as np
import re
import xlwt
import time

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('report_output.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('report_output.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

import net_value_cal_display
import compare_net_value
import fund_holdings_display


class report_output(object):

    def __init__(self):
        self.net_value_cal_display = net_value_cal_display.net_value_cal_display()
        self.compare_net_value = compare_net_value.compare_net_value()
        self.fund_holdings_display = fund_holdings_display.fund_holdings_display()

    def daily_report(self, fund_file):
        start_time = time.time()
        file_name = u'%s日报.xlsx' %datetime.datetime.now().strftime(u'%Y-%m-%d-')
        if os.path.exists(file_name):
            os.remove(file_name)
            wb = xlwt.Workbook()
            wb.add_sheet('sheet1')
            wb.save(file_name)

        writer = pd.ExcelWriter(file_name)

        net_value_cal_display = self.net_value_cal_display.daily_report(fund_file)

        df = pd.read_excel(net_value_cal_display, sheet_name=0)
        df.to_excel(writer, net_value_cal_display, index=None)
        # os.remove(net_value_cal_display)

        index_calculation = self.net_value_cal_display.index_calculation()

        df = pd.read_excel(index_calculation, sheet_name=0)
        df.to_excel(writer, u'指数计算', index=None)
        # os.remove(net_value_cal_display)

        compare_net_value1, compare_net_value2 = self.compare_net_value.daily_display(fund_file)

        df = pd.read_excel(compare_net_value1, sheet_name=0)
        df.to_excel(writer, u'热点基金净值拟合比较', index=None)
        # os.remove(compare_net_value1)

        df = pd.read_csv(compare_net_value2, encoding='utf8', index_col=None).T

        # 调整格式
        for i in range(df.shape[1]):
            if i % 5 in [0,1,2]:
                df.iloc[1:, i] = df.iloc[1:, i].astype(np.float64).round(4)
            elif i % 5 == 3:
                df.iloc[1:, i] = df.iloc[1:, i].apply(lambda s:"%s%s" %(round(float(s),4) * 100, "%") if not isinstance(s, type(np.nan)) else np.nan)
            elif i % 5 == 4:
                df.iloc[:, i] = df.iloc[:, i].apply(lambda s:datetime.datetime.strptime(s, '%Y-%m-%d').date() if not isinstance(s, type(np.nan)) else np.nan)
        # print df
        df.to_excel(writer, u'净值拟合数据')

        # fund_holdings_display = self.fund_holdings_display.display_all(fund_file)
        #
        # df = pd.read_csv(fund_holdings_display, index_col=None)
        # df.columns = ['' if re.search(r'Unnamed', s) else s for s in df.columns]
        # df.to_excel(writer, u'热点基金持股明细表', index=None)
        # os.remove(fund_holdings_display)

        writer.save()
        # 163212





if __name__ == '__main__':
    report_output = report_output()
    report_output.daily_report('important_fund.txt')
    # report_output.daily_report('weekly_fund.txt')  # C:\Users\Administrator\Desktop\fund_monitor\weekly_fund.txt