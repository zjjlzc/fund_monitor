# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: net_value_cal_display.py
    @time: 2017/11/18 23:21
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
from dateutil.relativedelta import relativedelta
import calendar
import json


sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('net_value_cal_display.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('net_value_cal_display.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class net_value_cal_display(object):
    def __init__(self):
        pass

    def get_data(self, code, date1, date2):
        date1 = date1.strftime('%Y-%m-%d')
        date2 = date2.strftime('%Y-%m-%d')
        sql = 'SELECT `crawler_key`, `fund_code`, `value_date`,`accumulative_net_value` FROM `eastmoney_daily_data` WHERE `fund_code` = %s AND (`value_date` BETWEEN "%s" AND "%s")' %(code,date1,date2)
        # print sql
        with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        # print df.head(5)
        return df

    def mul_cal(self, summary_listing, row, fund_code, date1, date2, earnings_type, needed_data, co_col):
        ser = self.simple_cal(fund_code, date1, date2, earnings_type)
        if ser is not None:
            for i in range(len(needed_data)):
                summary_listing.loc[row, needed_data[i]] = ser[co_col[i]]
        return summary_listing

    def simple_cal(self, fund_code, date1, date2, earnings_type):
        print u"正在准备计算%s的从%s到%s的%s数据" %(fund_code, date1, date2, earnings_type)
        # print 'mark00'
        date_col = u'value_date'
        value_col = u'accumulative_net_value'
        target_col = u'retracement'
        df = self.get_data(fund_code, date1, date2)  # 从数据库读取数据
        df = df.drop(df.index[df['accumulative_net_value'] == ''], axis=0)
        df.loc[:, date_col] = pd.to_datetime(df.loc[:, date_col])

        # 截取所需时间内的数据,数据取到小于date1的最后一个
        serx = df[date_col][df[date_col].between(datetime.datetime(1800, 1, 1), date1 - datetime.timedelta(1))]
        if not serx.empty:
            serx = serx.sort_values()
            date1 = serx.iat[-1]

        df = df[df[date_col].between(date1, date2)].copy()
        # 修正日期
        df = df.sort_values([date_col, ])  # 按时间排序
        if df.empty:
            print u'从eastmoney_daily_data读取的数据为空'
            return None
        date1 = df[date_col].iat[0]
        date2 = df[date_col].iat[-1]

        func_max = lambda date: max(df[value_col][df[date_col] <= date])
        df.index = range(df.shape[0])
        # print df
        df['accumulative_net_value'] = df['accumulative_net_value'].astype(np.float)
        for i in range(1, df.shape[0]):
            # print 'mark1'
            value0 = df.loc[i, value_col]
            date0 = df.loc[i, date_col]
            df.loc[i, 'max'] = func_max(date0)
            if value0 < func_max(date0):
                df.loc[i, target_col] = (value0 / func_max(date0)) - 1
            else:
                df.loc[i, target_col] = 0

        #df.to_csv('caculated_data.csv', encoding='utf_8_sig')

        if earnings_type == u'年化收益':
            product_yield = (df[value_col].iat[-1] / df[value_col].iat[0] - 1) * 365.0 / (
            df[date_col].iat[-1] - df[date_col].iat[0]).days  # 最后一个除以第一个，减1
        elif earnings_type == u'收益':
            product_yield = df[value_col].iat[-1] / df[value_col].iat[0] - 1  # 最后一个除以第一个，减1
        else:
            product_yield = None
        retracement = min(df[target_col].dropna())
        earnings_retracement_ratio = product_yield / (-retracement)

        return pd.Series([df['fund_code'].iat[0], product_yield, retracement, earnings_retracement_ratio],
                         index=[u'基金代号', earnings_type, u'回撤', u'收益回撤比'])

    def main(self):
        with open('important_fund.txt', 'r') as f:
            code_list = f.read().split('\n')

        # sql = """
        # SELECT DISTINCT `fund_code` FROM `eastmoney_daily_data`
        # """
        # with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
        #     df0 = pd.read_sql(sql, conn)
        # for code in df0.iloc[:,0].tolist():
        #     if code not in code_list:
        #         code_list.append(code)

        df = pd.DataFrame(code_list, columns=['fund_code',])
        df.index = df['fund_code']

        date_today = datetime.datetime.now()

        start_time = time.time()
        for code in df.index:
            # 基金类型,基金名称，基金分类
            sql = """
            SELECT `fund_code`, `fund_name`, `fund_type`, `1st_class`, `2nd_class`, `3rd_class` FROM `fund_info` WHERE `fund_code` = '%s'
            """ % code
            with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                df0 = pd.read_sql(sql, conn)

            df.loc[code, u'基金名称'] = df0['fund_name'].iloc[0]
            df.loc[code, u'基金类型'] = df0['fund_type'].iloc[0]
            df.loc[code, u'一级分类'] = df0['1st_class'].iloc[0]
            df.loc[code, u'二级分类'] = df0['2nd_class'].iloc[0]
            df.loc[code, u'三级分类'] = df0['3rd_class'].iloc[0]

            # 成立日期
            sql = """
            SELECT `fund_code`, `found_date` FROM `fund_info` WHERE `fund_code` = '%s'
            """ % code
            with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                df0 = pd.read_sql(sql, conn)

            df.loc[code, u'成立日期'] = df0['found_date'].iloc[0]

            # 基金基本信息
            sql = """
            SELECT `data_type`, `json_data` FROM `fund_mixed_data` WHERE `fund_code` = '%s' AND `data_type` in ('%s','%s','%s')
            """ % (code, u'规模变动', u'持有人结构', u'基金经理变动')
            with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                mul_data = pd.read_sql(sql, conn)

            # 规模变动
            df0 = mul_data[mul_data['data_type'] == u'规模变动'].copy()
            df0.loc[:,'value_date'] = df0.loc[:,'json_data'].apply(lambda s:json.loads(s)['value_date'])
            df0.loc[:,'data'] = df0.loc[:,'json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'净资产规模(亿)'])
            df0 = df0.sort_values(['value_date',], ascending=False)

            df.loc[code, u'规模变动(最新/6月末/3月末/最初)'] = '/'.join([str(num) for num in df0.iloc[[0,1,2,-1],:]['data'].tolist()])

            # 机构持有/个人持有
            df0 = mul_data[mul_data['data_type'] == u'持有人结构'].copy()
            df0['value_date'] = df0['json_data'].apply(lambda s: json.loads(s)['value_date'])
            df0[u'机构持有比例'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'机构持有比例'])
            df0[u'个人持有比例'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'个人持有比例'])
            df0 = df0.sort_values(['value_date', ], ascending=False)

            df.loc[code, u'机构持有/个人持有'] = str(df0[u'机构持有比例'].iloc[0]) + '/' + str(df0[u'个人持有比例'].iloc[0])

            # 基金经理变动
            df0 = mul_data[mul_data['data_type'] == u'基金经理变动'].copy()
            df0[u'基金经理'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'基金经理']['0'])

            df.loc[code, u'基金经理'] = df0[u'基金经理'].iloc[0]


            # 预测净值
            date_str = date_today.strftime('%Y-%m-%d')
            sql = """
            SELECT `value_date`,`estimate_net_value`,`estimate_daily_growth_rate`,`net_asset_value`,`daily_growth_rate` FROM `eastmoney_daily_data`
            WHERE `value_date` <= "%s" AND `fund_code` = "%s"
            ORDER BY `value_date` DESC
            LIMIT 1
            """ %(date_str,code)
            with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                df0 = pd.read_sql(sql, conn)

            datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
            df.loc[code, datex + u"预估净值"] = df0.loc[0, 'estimate_net_value'] if not df0.empty else None
            df.loc[code, datex + u"预估净值收益率"] = df0.loc[0, 'estimate_daily_growth_rate'] if not df0.empty else None
            df.loc[code, datex + u"结算净值"] = df0.loc[0, 'net_asset_value'] if not df0.empty else None
            df.loc[code, datex + u"结算净值收益率"] = df0.loc[0, 'daily_growth_rate'] if not df0.empty else None


            # 近两月，月底单位净值与累计净值
            date_list = [datetime.datetime(year=date_today.year,month=(date_today - relativedelta(months=i)).month,
                                           day=calendar.monthrange(date_today.year,(date_today - relativedelta(months=i)).month)[1]) for i in [1,2]]
            for date0 in date_list:
                date_str = date0.strftime('%Y-%m-%d')
                # 万一没有数据，不会打乱格式
                sql = """
                SELECT `value_date`, `net_asset_value`, `accumulative_net_value` FROM `eastmoney_daily_data`
                WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                ORDER BY `value_date` DESC
                LIMIT 1
                """ %(date_str,code)
                #print sql
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                df.loc[code, datex + u"单位净值"] = df0.loc[0, 'net_asset_value'] if not df0.empty else None
                df.loc[code, datex + u"累计净值"] = df0.loc[0, 'accumulative_net_value'] if not df0.empty else None


            # 近三天，单位净值
            for date0 in [date_today+relativedelta(days=-i) for i in [1,2,3]]:
                date_str = date0.strftime('%Y-%m-%d')
                #df[date0.strftime(u'%m月%d日') + "累计净值"] = None # 万一没有数据，不会打乱格式
                sql = """
                SELECT `value_date`, `net_asset_value`, `accumulative_net_value` FROM `eastmoney_daily_data`
                WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                ORDER BY `value_date` DESC
                LIMIT 1
                """ %(date_str,code)
                #print sql
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                df.loc[code, date_str + "单位净值"] = df0.loc[0, 'net_asset_value'] if not df0.empty else None
                #df.loc[code, datex + u"累计净值"] = df0.loc[0, 'accumulative_net_value'] if not df0.empty else None

            # 今天与上个月底的股票仓位预测
            date_1 = datetime.datetime(year=date_today.year, month=(date_today - relativedelta(months=1)).month,
                              day=calendar.monthrange(date_today.year, (date_today - relativedelta(months=1)).month)[1])
            for date0 in [date_1, date_today]:
                sql = """
                SELECT `value_date`, `fund_shares_positions` FROM `eastmoney_daily_data`
                WHERE `value_date` <= "%s" AND `fund_code` = "%s"
                ORDER BY `value_date` DESC
                LIMIT 1
                """ % (date0.strftime('%Y-%m-%d'), code)
                with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                    df0 = pd.read_sql(sql, conn)

                datex = df0.loc[0, 'value_date'].strftime(u'%m月%d日')
                df.loc[code, datex + u"股票仓位"] = df0.loc[0, 'fund_shares_positions'] if not df0.empty else None

            # 今天日收益
            ser = self.simple_cal(code, date_today, date_today,u'收益')
            df.loc[code, u'%s收益率' %date_today.strftime(u'%m月%d日')] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 近两个月的月收益率
            for date1 in [datetime.datetime(year=date_today.year,month=(date_today - relativedelta(months=i)).month,day=1) for i in [1,2]]:
                date2 = datetime.datetime(year=date1.year, month=date1.month, day=calendar.monthrange(date1.year,month=date1.month)[1])
                ser = self.simple_cal(code, date1, date2,u'收益')
                df.loc[code, u'%s年%s月收益' %(date1.year,date1.month)] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 今年的季度数据
            for month1 in [10,7,4,1]:
                month2 = month1 + 2
                date1 = datetime.datetime(year=date_today.year,month=month1,day=1)
                date2 = datetime.datetime(year=date_today.year,month=month2,day=calendar.monthrange(date_today.year,month=month2)[1])
                ser = self.simple_cal(code, date1, date2, u'收益')
                df.loc[code, u'%s年%s月-%s月收益' % (date1.year, date1.month, date2.month)] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 去年最后三个月的收益率
            for month0 in [12,11,10]:
                date1 = datetime.datetime(year=date_today.year-1, month=month0, day=1)
                date2 = datetime.datetime(year=date_today.year-1, month=month0, day=calendar.monthrange(date_today.year-1, month=month0)[1])
                ser = self.simple_cal(code, date1, date2, u'收益')
                df.loc[code, u'%s年%s月收益' % (date1.year, date1.month)] = ser[u'收益'] if ser is not None and not ser.empty else None

            # 近一周收益率
            date1 = date_today - datetime.timedelta(7)
            date2 = date_today
            ser_1w = self.simple_cal(code, date1, date2, u'收益')
            # 近一个月 收益率，回撤，收益回撤比
            date1 = date_today + relativedelta(months=-1)
            date2 = date_today
            ser_1m = self.simple_cal(code, date1, date2, u'收益')
            # 近三个月 收益率，回撤，收益回撤比
            date1 = date_today + relativedelta(months=-3)
            date2 = date_today
            ser_3m = self.simple_cal(code, date1, date2, u'收益')
            # 近六个月 收益率，回撤，收益回撤比
            date1 = date_today + relativedelta(months=-6)
            date2 = date_today
            ser_6m = self.simple_cal(code, date1, date2, u'收益')
            # 近一年 收益率，回撤，收益回撤比
            date1 = date_today + relativedelta(years=-1)
            date2 = date_today
            ser_1y = self.simple_cal(code, date1, date2, u'收益')
            # 近两年 收益率，回撤，收益回撤比
            date1 = date_today + relativedelta(years=-2)
            date2 = date_today
            ser_2y = self.simple_cal(code, date1, date2, u'收益')
            # 今年以来 收益率，回撤，收益回撤比
            date1 = datetime.datetime(year=date_today.year, month=1, day=1)
            date2 = date_today
            ser_this_year = self.simple_cal(code, date1, date2, u'收益')
            # 成立以来 收益率，回撤，收益回撤比
            date1 = datetime.datetime(year=1900, month=1, day=1)
            date2 = date_today
            ser_so_far = self.simple_cal(code, date1, date2, u'收益')
            # 2016年2月29日以来 收益率，回撤，收益回撤比
            date1 = datetime.datetime(year=2016, month=2, day=29)
            date2 = date_today
            ser_20160229 = self.simple_cal(code, date1, date2, u'收益')

            df.loc[code, u'近一周收益率'] = ser_1w[u'收益'] if ser_1w is not None and not ser_1w.empty else None
            df.loc[code, u'近一个月收益率'] = ser_1m[u'收益'] if ser_1m is not None and not ser_1m.empty else None
            df.loc[code, u'近三个月收益率'] = ser_3m[u'收益'] if ser_3m is not None and not ser_3m.empty else None
            df.loc[code, u'近六个月收益率'] = ser_6m[u'收益'] if ser_6m is not None and not ser_6m.empty else None
            df.loc[code, u'近一年收益率'] = ser_1y[u'收益'] if ser_1y is not None and not ser_1y.empty else None
            df.loc[code, u'近两年收益率'] = ser_2y[u'收益'] if ser_2y is not None and not ser_2y.empty else None
            df.loc[code, u'今年以来收益率'] = ser_this_year[u'收益'] if ser_this_year is not None and not ser_this_year.empty else None
            df.loc[code, u'成立以来收益率'] = ser_so_far[u'收益'] if ser_so_far is not None and not ser_so_far.empty else None
            df.loc[code, u'2016年2月29日以来收益率'] = ser_20160229[u'收益'] if ser_20160229 is not None and not ser_20160229.empty else None


            df.loc[code, u'近一个月回撤'] = ser_1m[u'回撤'] if ser_1m is not None and not ser_1m.empty else None
            df.loc[code, u'近三个月回撤'] = ser_3m[u'回撤'] if ser_3m is not None and not ser_3m.empty else None
            df.loc[code, u'近六个月回撤'] = ser_6m[u'回撤'] if ser_6m is not None and not ser_6m.empty else None
            df.loc[code, u'近一年回撤'] = ser_1y[u'回撤'] if ser_1y is not None and not ser_1y.empty else None
            df.loc[code, u'近两年回撤'] = ser_2y[u'回撤'] if ser_2y is not None and not ser_2y.empty else None
            df.loc[code, u'今年以来回撤'] = ser_this_year[u'回撤'] if ser_this_year is not None and not ser_this_year.empty else None
            df.loc[code, u'成立以来回撤'] = ser_so_far[u'回撤'] if ser_so_far is not None and not ser_so_far.empty else None
            df.loc[code, u'2016年2月29日以来回撤'] = ser_20160229[u'回撤'] if ser_20160229 is not None and not ser_20160229.empty else None


            df.loc[code, u'近一年收益回撤比'] = ser_1y[u'收益回撤比'] if ser_1y is not None and not ser_1y.empty else None
            df.loc[code, u'近两年收益回撤比'] = ser_2y[u'收益回撤比'] if ser_2y is not None and not ser_2y.empty else None
            df.loc[code, u'今年以来收益回撤比'] = ser_this_year[u'收益回撤比'] if ser_this_year is not None and not ser_this_year.empty else None
            df.loc[code, u'成立以来收益回撤比'] = ser_so_far[u'收益回撤比'] if ser_so_far is not None and not ser_so_far.empty else None
            df.loc[code, u'2016年2月29日以来收益回撤比'] = ser_20160229[u'收益回撤比'] if ser_20160229 is not None and not ser_20160229.empty else None

            sql = """
            SELECT `fund_code`, `data_type`, `json_data` FROM `fund_mixed_data` WHERE `fund_code` = '%s' AND `data_type` = '%s'
            """ %(code, u'资产配置')
            with closing(pymysql.connect('10.10.10.15', 'spider', 'jlspider', 'spider', charset='utf8')) as conn:
                df0 = pd.read_sql(sql, conn)

            df0['value_date'] = df0['json_data'].apply(lambda s: json.loads(s)['value_date'])
            df0[u'股票占净比'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'股票占净比'])
            df0[u'债券占净比'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'债券占净比'])
            df0[u'现金占净比'] = df0['json_data'].apply(lambda s: json.loads(s, encoding='utf8')[u'现金占净比'])
            df0 = df0.sort_values(['value_date', ], ascending=False)

            func = lambda s:datetime.datetime.strptime(s, '%Y-%m-%d').strftime('%m月%d日')
            for i in range(df0.shape[0])[:2]:
                df.loc[code, u'%s股票占净比' %func(df0['value_date'].iloc[i])] = df0[u'股票占净比'].iloc[i]
                df.loc[code, u'%s债券占净比' %func(df0['value_date'].iloc[i])] = df0[u'债券占净比'].iloc[i]
                df.loc[code, u'%s现金占净比' %func(df0['value_date'].iloc[i])] = df0[u'债券占净比'].iloc[i]

            #date1 = datetime.datetime(1900, 1, 1)
            #date2 = datetime.datetime(2017, 10, 31)
            #summary_listing = self.simple_cal(code, date1, date2, u'年化收益')

            df.to_csv('net_value_cal_display.csv', encoding='ascii')
            print u"耗时:", time.time() - start_time





if __name__ == '__main__':
    net_value_cal_display = net_value_cal_display()
    net_value_cal_display.main()