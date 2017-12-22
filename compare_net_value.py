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

import re
import xlrd
import xlwt
from xlutils.copy import copy

import cal_fitting_net_value
cal_fitting_net_value = cal_fitting_net_value.cal_fitting_net_value()
import requests_manager
requests_manager = requests_manager.requests_manager()
import json
import time
import calculation
calculation = calculation.calculation()

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('compare_net_value.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('compare_net_value.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

user_key = '1923eae2a3054d4c92a7eb74d7f65396'


class compare_net_value(object):
    def __init__(self):
        pass


    def get_net_value(self, fund_code, date_str1, date_str2):
        sql = "SELECT `fund_code`, `value_date`, `accumulative_net_value` FROM `eastmoney_daily_data` " \
              "WHERE `fund_code` = '%s' AND `value_date` BETWEEN '%s' AND '%s'" %(fund_code, date_str1, date_str2)

        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn, index_col='value_date')
        df.index = [date0.strftime('%Y-%m-%d') for date0 in df.index]
        df = df.drop(df[df['accumulative_net_value'] == ''].index.tolist(), axis=0)
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
        with open(r'important_fund.json', 'r') as f:
            fund_code_json = json.load(f, encoding='gbk')
        #fund_code_list = ['001542',]
        date_l = [
            ['2016-09-30', '2016-10-20', '2017-04-19'],
            ['2017-03-31', '2017-04-20', '2017-08-20'],
            ['2017-06-30', '2017-08-21', '2017-10-20'],
            ['2017-09-30', '2017-10-21', '2020-01-01'],
        ]
        if os.path.exists(u'compare_net_value_计算过程.csv'):
            os.remove(u'compare_net_value_计算过程.csv')

        title_set = set()
        for title in fund_code_json:
            title_set.add(title)
            fund_code_list = fund_code_json[title]
            output = pd.DataFrame([])
            for fund_code in fund_code_list:
                print u"正在计算基金：", fund_code
                start_time = time.time()
                try:
                    web_net_value = self.get_net_value(fund_code, date_l[0][1], date_l[-1][-1])
                    cal_net_value = cal_fitting_net_value.combine_net_value(fund_code, date_l)
                    df = web_net_value.join(cal_net_value)

                    df = df.dropna()
                    df.loc[:, 'cal_net_asset_value'] = df.loc[:, 'accumulative_net_value'].apply(lambda x:float(x)/float(df['accumulative_net_value'].iat[0]))

                    df = df.reindex(['accumulative_net_value', 'cal_net_asset_value', 'fitting_net_value'], axis=1)

                    # 改成中文标题后输出，源数据不改
                    # df['fund_code'] = fund_code
                    df.rename({
                        'fund_code': fund_code + u'基金代码',
                        'accumulative_net_value': fund_code + u'基金净值',
                        'cal_net_asset_value': fund_code + u'折算基金净值',
                        'fitting_net_value': fund_code + u'拟合净值'}, axis=1).T.to_csv(u'compare_net_value_计算过程.csv', mode='a')

                    func = lambda ser, date_str1, date_str2: ser[(pd.to_datetime(ser.index) >= datetime.datetime.strptime(date_str1, '%Y-%m-%d')) & (
                                                                  pd.to_datetime(ser.index) <= datetime.datetime.strptime(date_str2, '%Y-%m-%d'))]

                    print u'计算全期数据'
                    df['value_date'] = df.index.tolist()
                    ser = calculation.earnings_cal(fund_code, df, date_col='value_date', value_col='fitting_net_value', data_type='fund') #self.simple_cal(df['fitting_net_value'],u'收益率')
                    ser = ser.drop([u'基金代号', ])
                    ser1 = ser.rename({key:u'拟合净值' + key for key in ser.index}).copy()

                    ser = calculation.earnings_cal(fund_code, df, date_col='value_date', value_col='cal_net_asset_value', data_type='fund') # self.simple_cal(df['cal_net_asset_value'],u'收益率')
                    ser = ser.drop([u'基金代号', ])
                    ser2 = ser.rename({key:u'折算净值' + key for key in ser.index}).copy()#ser.name = u'折算净值收益率'
                    ser_total = ser1.append(ser2)

                    i = 1
                    ser_period = pd.Series([])
                    ser_compare = pd.Series([])


                    # 按照基金持仓报告，最后一季的持仓到现在的效果
                    date1 = datetime.datetime.strptime(date_l[-1][0], '%Y-%m-%d')
                    date2 = datetime.datetime.strptime(date_l[-1][-1], '%Y-%m-%d')
                    ser = calculation.earnings_cal(fund_code, df, date_col='value_date', value_col='fitting_net_value', date1=date1,
                                                   date2=date2, data_type='fund')  # self.simple_cal(func(df['fitting_net_value'], date_str1, date_str2), u'收益率')
                    if ser is None:
                        continue
                    ser = ser[[u'收益率', ]].copy()
                    ser = ser.rename({key: u'近期拟合净值' + key for key in ser.index})  # ser.name = u'拟合净值收益率' + str(i)
                    ser_period = ser_period.append(ser)

                    ser = calculation.earnings_cal(fund_code, df, date_col='value_date', value_col='cal_net_asset_value',
                                                   date1=date1, date2=date2, data_type='fund')
                    if ser is None:
                        continue
                    ser = ser[[u'收益率', ]].copy()
                    ser = ser.rename({key: u'近期折算净值' + key for key in ser.index})  # ser.name = u'折算净值收益率' + str(i)
                    ser_period = ser_period.append(ser)

                    ser_compare = ser_compare.append(
                        pd.Series([(ser_period[u'近期拟合净值收益率'] - ser_period[u'近期折算净值收益率']), ], index=[u"近期收益比较", ]))

                    # 区间内拟合效果比较
                    for cut_off_day, date_str1, date_str2 in reversed(date_l):
                        #print cut_off_day, date_str1, date_str2
                        date1 = datetime.datetime.strptime(date_str1, '%Y-%m-%d')
                        date2 = datetime.datetime.strptime(date_str2, '%Y-%m-%d')
                        ser = calculation.earnings_cal(fund_code, df, date_col='value_date', value_col='fitting_net_value',
                                                       date1=date1, date2=date2, data_type='fund')# self.simple_cal(func(df['fitting_net_value'], date_str1, date_str2), u'收益率')
                        if ser is None:
                            continue
                        ser = ser[[u'收益率',]].copy()
                        ser = ser.rename({key:u'拟合净值' + key + str(i) for key in ser.index})#ser.name = u'拟合净值收益率' + str(i)
                        ser_period = ser_period.append(ser)

                        ser = calculation.earnings_cal(fund_code, df, date_col='value_date', value_col='cal_net_asset_value',
                                                       date1=date1, date2=date2, data_type='fund')
                        if ser is None:
                            continue
                        ser = ser[[u'收益率',]].copy()
                        ser = ser.rename({key:u'折算净值' + key + str(i) for key in ser.index})#ser.name = u'折算净值收益率' + str(i)
                        ser_period = ser_period.append(ser)

                        ser_compare = ser_compare.append(pd.Series([(ser_period[u'拟合净值收益率%s' %i] - ser_period[u'折算净值收益率%s' %i]), ], index=[u"区间%s收益比较" %i,]))
                        i = i + 1

                    res = pd.Series([])
                    sql = """
                    SELECT `fund_code`, `fund_name`, `2nd_class`, `3rd_class` FROM `fund_info` WHERE `fund_code` = '%s'
                    """ % fund_code
                    with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                        df0 = pd.read_sql(sql, conn)

                    res = res.append(df0.iloc[0,:].reindex(['2nd_class', '3rd_class', 'fund_code', 'fund_name']).\
                                     rename({'2nd_class':u'二级分类', '3rd_class':u'三级分类', 'fund_code':u'基金代号', 'fund_name':u'基金名称'}))

                    res = res.append(pd.Series([ser_total[u'拟合净值收益率'] - ser_total[u'折算净值收益率'],
                                                ser_compare[ser_compare.abs() == min(ser_compare.abs())].index.tolist()[0].replace(u'收益比较',''),
                                                ser_compare[ser_compare.abs() == min(ser_compare.abs())].iloc[0],
                                                ser_total[u'拟合净值收益率'],
                                                ser_total[u'折算净值收益率']
                                                ],
                                               index= [
                                                   u'拟合净值收益-实际净值收益',
                                                   u'收益最小差距区间',
                                                   u'最小收益差距',
                                                   u'拟合净值收益率',
                                                   u'折算净值收益率'
                                                ]))
                    res = res.append(ser_period)
                    res = res.append(ser_compare)

                    res = res.append(pd.Series([ser_total[u'拟合净值年化收益率'],
                                                ser_total[u'折算净值年化收益率'],
                                                ser_total[u'拟合净值最大回撤'],
                                                ser_total[u'折算净值最大回撤'],
                                                ser_total[u'拟合净值收益回撤比'],
                                                ser_total[u'折算净值收益回撤比'],

                                                ],
                                               index= [
                                                   u'拟合净值年化收益率',
                                                   u'折算净值年化收益率',
                                                   u'拟合净值最大回撤',
                                                   u'折算净值最大回撤',
                                                   u'拟合净值收益回撤比',
                                                   u'折算净值收益回撤比'
                                                ]))

                    output = output.append(res, ignore_index=True)
                    output_colums = res.index.tolist() if len(res.index.tolist()) >= output.shape[1] else output.columns.tolist()
                    output = output.reindex(output_colums, axis=1)
                    print output


                    output.to_excel('compare_net_value_%s.xls' %title, index=None)

                    oldWb = xlrd.open_workbook('compare_net_value_%s.xls' %title)
                    added_row = oldWb.sheet_by_index(0).nrows
                    newWb = copy(oldWb)
                    newWs = newWb.get_sheet(0)
                    c = 0
                    for l in reversed(date_l):
                        newWs.write(added_row , c, u'区间%s：%s 至 %s' %(c/4+1, l[1],l[2]))
                        c = c + 4
                    newWb.save('compare_net_value_%s.xls' %title)

                #print pd.Series([fund_code, ], index=['fund_code']).append(res)



                # print df
                # dfx = df.T.copy()

                # func = lambda ser, date_str1, date_str2:ser[(pd.to_datetime(ser.index)>=datetime.datetime.strptime(date_str1,'%Y-%m-%d')) & (pd.to_datetime(ser.index)<=datetime.datetime.strptime(date_str2,'%Y-%m-%d'))]
                # for cut_off_day, date_str1, date_str2 in date_l:
                #     ser = self.simple_cal(func(df['fitting_net_value'], date_str1, date_str2), u'收益率')
                #     ser.name = u'拟合净值收益率' + date_str1  + '/' + date_str2
                #     ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                #     dfx = dfx.join(ser)
                #
                #     ser = self.simple_cal(func(df['fitting_net_value'], date_str1, date_str2), u'年化收益率')
                #     ser.name = u'拟合净值年化收益率' + date_str1  + '/' + date_str2
                #     ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                #     dfx = dfx.join(ser)
                #
                #     ser = self.simple_cal(func(df['cal_net_asset_value'], date_str1, date_str2), u'收益率')
                #     ser.name = u'折算净值收益率' + date_str1  + '/' + date_str2
                #     ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                #     dfx = dfx.join(ser)
                #
                #     ser = self.simple_cal(func(df['cal_net_asset_value'], date_str1, date_str2), u'年化收益率')
                #     ser.name = u'折算净值年化收益率' + date_str1  + '/' + date_str2
                #     ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                #     dfx = dfx.join(ser)
                #
                # ser = self.simple_cal(df['fitting_net_value'],u'收益率')
                # ser.name = fund_code + u'拟合净值收益率'
                # ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                # dfx = dfx.join(ser)
                #
                # ser = self.simple_cal(df['fitting_net_value'],u'年化收益率')
                # ser.name = fund_code + u'拟合净值年化收益率'
                # ser.index = ['fund_code', 'accumulative_net_value','cal_net_asset_value']
                # dfx = dfx.join(ser)
                #
                # ser = self.simple_cal(df['cal_net_asset_value'],u'收益率')
                # ser.name = fund_code + u'折算净值收益率'
                # ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                # dfx = dfx.join(ser)
                #
                # ser = self.simple_cal(df['cal_net_asset_value'],u'年化收益率')
                # ser.name = fund_code + u'折算净值年化收益率'
                # ser.index = ['fund_code', 'accumulative_net_value', 'cal_net_asset_value']
                # dfx = dfx.join(ser)
                #
                # dfx = dfx.rename({'fund_code': u'收益率', 'accumulative_net_value': u'回撤率', 'fitting_net_value': '', 'cal_net_asset_value': u'收益回撤比'},axis=0)
                # dfx.reindex(reversed(dfx.columns), axis=1).to_csv('compare_net_value0.csv', mode='a')
                #
                #
                # df_final = pd.read_csv('compare_net_value0.csv').T
                # df_final.to_csv('compare_net_value1.csv')

                    print u"耗时：", time.time() - start_time
                except:
                    log_obj.error('%s计算时出错' %fund_code)
                    log_obj.error(traceback.format_exc())

            if os.path.exists('compare_net_value.xls'):
                os.remove('compare_net_value.xls')
                wb = xlwt.Workbook()
                wb.add_sheet('sheet1')
                wb.save('compare_net_value.xls')

            writer = pd.ExcelWriter('compare_net_value.xls')
            for title in title_set:
                df = pd.read_excel(u'compare_net_value_%s.xls' %title, sheet_name=0)
                df.to_excel(writer, title, index=None)
            writer.save()

    def daily_display(self, fund_file='weekly_fund.txt'):

        with open(fund_file, 'r') as f:
            fund_code_list = f.read().split('\n')

        date_l = [
            # ['2016-09-30', '2016-10-20', '2017-04-19'],
            # ['2017-03-31', '2017-04-20', '2017-08-20'],
            # ['2017-06-30', '2017-08-21', '2017-10-20'],
            ['2017-09-30', '2017-09-29', datetime.datetime.now().strftime('%Y-%m-%d')],
        ]
        if os.path.exists(u'compare_net_value_计算过程.csv'):
            os.remove(u'compare_net_value_计算过程.csv')

        output = pd.DataFrame([])
        for fund_code in fund_code_list:

            if not fund_code:
                output.loc[fund_code, :] = None
                print u'空白行，略过'
                continue

            if re.search(r'[^\d]+', fund_code):
                output.loc[fund_code.decode('gbk'), u'基金代号'] = fund_code.decode('gbk')
                continue

            print u"正在计算基金：", fund_code
            start_time = time.time()
            try:
                web_net_value = self.get_net_value(fund_code, date_l[0][1], date_l[-1][-1])
                cal_net_value = cal_fitting_net_value.combine_net_value(fund_code, date_l)
                df = web_net_value.join(cal_net_value)

                df = df.dropna()
                df.loc[:, 'cal_net_asset_value'] = df.loc[:, 'accumulative_net_value'].apply(
                    lambda x: float(x) / float(df['accumulative_net_value'].iat[0]))

                df = df.reindex(['cal_net_asset_value', 'fitting_net_value', 'accumulative_net_value'], axis=1)

                # 改成中文标题后输出，源数据不改
                # df['fund_code'] = fund_code
                output_df0 = df.copy()
                output_df0[u'比较'] = [datetime.datetime.strptime(date, '%Y-%m-%d').isoweekday() for date in output_df0.index]


                rate = ((output_df0[output_df0[u'比较']==5])['accumulative_net_value'].astype(np.float) / (output_df0[output_df0[u'比较']==5])['accumulative_net_value'].shift(1).astype(np.float) -1).copy()
                # print (output_df0[output_df0[u'比较']==5])['accumulative_net_value'].astype(np.float)
                # print (output_df0[output_df0[u'比较']==5])['accumulative_net_value'].shift(1).astype(np.float)
                # print rate
                output_df0[u'比较'] = rate
                # print output_df0

                sql = """
                SELECT `fund_code`, `fund_name` FROM `fund_info`
                WHERE `fund_code` = '%s'
                """ %fund_code
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)
                    fund_name = df0['fund_name'].iloc[0]

                output_df0.rename({
                    'fund_code': fund_name + u'基金代码',
                    'accumulative_net_value': fund_name + u'基金净值',
                    'cal_net_asset_value': fund_name + u'折算基金净值',
                    'fitting_net_value': fund_name + u'拟合净值'}, axis=1).T.to_csv(u'compare_net_value_计算过程.csv', mode='a')

                func = lambda ser, date_str1, date_str2: ser[
                    (pd.to_datetime(ser.index) >= datetime.datetime.strptime(date_str1, '%Y-%m-%d')) & (
                            pd.to_datetime(ser.index) <= datetime.datetime.strptime(date_str2, '%Y-%m-%d'))]

                print u'计算全期数据'
                df['value_date'] = df.index.tolist()
                ser = calculation.earnings_cal(fund_code, df, date_col='value_date',
                                               value_col='fitting_net_value', data_type='fund')  # self.simple_cal(df['fitting_net_value'],u'收益率')
                ser = ser.drop([u'基金代号', ])
                ser1 = ser.rename({key: u'拟合净值' + key for key in ser.index}).copy()

                ser = calculation.earnings_cal(fund_code, df, date_col='value_date',
                                               value_col='cal_net_asset_value', data_type='fund')  # self.simple_cal(df['cal_net_asset_value'],u'收益率')
                ser = ser.drop([u'基金代号', ])
                ser2 = ser.rename({key: u'折算净值' + key for key in ser.index}).copy()  # ser.name = u'折算净值收益率'
                ser_total = ser1.append(ser2)

                res = pd.Series([])
                sql = """
                SELECT `fund_code`, `fund_name`, `2nd_class`, `3rd_class` FROM `fund_info` WHERE `fund_code` = '%s'
                """ % fund_code
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                res = res.append(df0.iloc[0, :].reindex(['fund_code', 'fund_name']).rename({'fund_code': u'基金代号', 'fund_name': u'基金名称'}))

                res = res.append(pd.Series([ser_total[u'拟合净值收益率'] - ser_total[u'折算净值收益率'],
                                            ser_total[u'拟合净值收益率'],
                                            ser_total[u'折算净值收益率']
                                            ],
                                           index=[
                                               u'收益率比较',
                                               u'拟合净值收益率',
                                               u'折算净值收益率'
                                           ]))

                res = res.append(pd.Series([ser_total[u'拟合净值最大回撤'],
                                            ser_total[u'折算净值最大回撤'],
                                            ser_total[u'拟合净值收益回撤比'],
                                            ser_total[u'折算净值收益回撤比'],

                                            ],
                                           index=[
                                               u'拟合净值最大回撤',
                                               u'折算净值最大回撤',
                                               u'拟合净值收益回撤比',
                                               u'折算净值收益回撤比'
                                           ]))

                output = output.append(res, ignore_index=True)
                output_colums = res.index.tolist() if len(res.index.tolist()) >= output.shape[1] else output.columns.tolist()
                output = output.reindex(output_colums, axis=1)
                print output

                output.to_excel('compare_net_value.xls')

                print u"耗时：", time.time() - start_time
            except:
                log_obj.error('%s计算时出错' % fund_code)
                log_obj.error(traceback.format_exc())

        return 'compare_net_value.xls', u'compare_net_value_计算过程.csv'


    def stock_fitting(self, date1='2017-10-21', date2='2018-01-01'):
        # df = pd.read_json('stock_fitting_data.json', dtype=np.str)
        with open('stock_fitting_data.json', 'r') as f:
            json_data = json.load(f)

        if os.path.exists(u'stock_fitting_计算过程.csv'):
            os.remove(u'stock_fitting_计算过程.csv')

        df_total = pd.DataFrame([])
        for code in json_data:
            ser = cal_fitting_net_value.single_fitting_net_value(json_data[code],date1,date2)
            ser = ser.dropna()
            ser.name = code
            df = pd.DataFrame(ser)
            df.T.to_csv(u'stock_fitting_计算过程.csv', mode='a')

            df = df.reset_index()
            df_total = df_total.append(calculation.earnings_cal(code, df, date_col='value_date', value_col=code, data_type='fund'), ignore_index=True)
        print df_total

    def estimate_vs_fitting(self):
        with open(r'important_fund2.txt', 'r') as f:
            fund_code_list = f.read().split('\n')

        date_l = [
            # ['2016-09-30', '2016-10-20', '2017-04-19'],
            # ['2017-03-31', '2017-04-20', '2017-08-20'],
            # ['2017-06-30', '2017-08-21', '2017-10-20'],
            # ['2017-09-30', '2017-10-21', '2020-01-01'],
            ['2017-09-30', '2017-09-29', '2020-01-01'],
        ]
        os.remove(u'estimate_vs_fitting_计算过程.csv') if os.path.exists(u'estimate_vs_fitting_计算过程.csv') else None
        os.remove('estimate_vs_fitting.csv') if os.path.exists(u'estimate_vs_fitting_计算过程.csv') else None

        summary_df = pd.DataFrame([])
        start_time = time.time()
        for fund_code in fund_code_list:
            if not fund_code or re.search(r'^[^\d]+$', fund_code):
                summary_df.loc[fund_code, :] = ''
                print u'空白或标题行，略过'
                continue

            sql = """
            SELECT `fund_code`, `value_date`, `estimate_daily_growth_rate` FROM `eastmoney_daily_data`
            WHERE `fund_code` = '%s'
            ORDER BY `value_date` DESC
            LIMIT 1
            """ %fund_code
            with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                estimate_df = pd.read_sql(sql, conn)
            summary_df.loc[fund_code, u'日期'] = estimate_df['value_date'].iloc[0]
            summary_df.loc[fund_code, u'预测净值日增长'] = estimate_df['estimate_daily_growth_rate'].iloc[0]

            cal_net_value = cal_fitting_net_value.combine_net_value(fund_code, date_l)
            cal_net_value.index = map(lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), cal_net_value.index.tolist())
            cal_net_value = cal_net_value[cal_net_value.index <= estimate_df['value_date'].iloc[0]]
            cal_net_value.name = fund_code

            summary_df.loc[fund_code, u'拟合净值日增长'] = str(round((cal_net_value.iloc[-1] / cal_net_value.iloc[-2] - 1) * 100, 2)) + '%'

            print summary_df

            pd.DataFrame(cal_net_value).T.to_csv(u'estimate_vs_fitting_计算过程.csv', mode='a')
            summary_df.to_csv('estimate_vs_fitting.csv', encoding='ascii')

            print u'耗时:', time.time() - start_time


if __name__ == '__main__':
    compare_net_value = compare_net_value()
    # compare_net_value.data_display()
    # compare_net_value.stock_fitting()
    compare_net_value.daily_display()