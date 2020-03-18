#!/use/bin/dev python
# -*- coding: utf-8 -*-
import os
import tqdm
import time
import pickle
import traceback
import pandas as pd
from datetime import timedelta
from multiprocessing import Process
from gm.api import set_token, history
from MyCTP.symbol_tools.symbol_tools import symbol_tools

# 合约工具
st = symbol_tools()

class Gm_download_one_day():
    """ 使用掘金下载一天的tick数据 """

    def __init__(self, token, trading_day, symbol_list, info=''):
        """ 初始化 """
        # 设置token
        set_token(token)

        self.__no_trading_flag = False
        if trading_day.weekday() >= 5:
            # 如果交易日是周六和周日, 就跳过
            print("\033[0;36;42m交易日为周六或周一, 不交易!\033[0m")
            self.__no_trading_flag = True

        self.__is_monday_flag = False
        if trading_day.weekday() == 0:
            # 如果是周一
            self.__is_monday_flag = True

        """ 交易日变量 """
        # 交易日
        self.trading_day = trading_day
        if self.__is_monday_flag:
            # 是周一
            # 周一字符串
            self.trading_day_monday_str = trading_day.strftime('%Y-%m-%d')
            # 周五字符串
            self.trading_day_friday_str = (trading_day - timedelta(days=3)).strftime('%Y-%m-%d')
            # 周六字符串
            self.trading_day_saturday_str = (trading_day - timedelta(days=2)).strftime('%Y-%m-%d')

        # 字符串交易日年月日字符串
        self.trading_day_str = trading_day.strftime('%Y-%m-%d')
        # 交易日前一天年月日字符串
        self.trading_day_yesterday_str = (trading_day - timedelta(days=1)).strftime('%Y-%m-%d')

        """ 系统变量 """
        # 需要处理的合约列表
        self.symbol_list = symbol_list

        # 保存路径
        self.save_path = './download/{0}'.format(self.trading_day_str)

        # 任务信息
        self.info = info

        # 新建目录
        try:
            if os.path.exists(self.save_path):
                if os.path.isdir(self.save_path):
                    pass
                elif os.path.isfile(self.save_path):
                    raise
            else:
                os.makedirs(self.save_path)
        except FileExistsError:
            pass

    def save_one_symbol(self, symbol):
        """ 保存一个合约tick数据 """
        try:
            print("正在下载: {0}".format(symbol))
            # 数据分3段下载, 每次只能下载33000根
            # 20:00 到00:00
            history_data_1 = history(symbol=symbol, frequency='tick', start_time='{0} 20:00'.format(self.trading_day_yesterday_str), end_time='{0} 00:00'.format(self.trading_day_str), df=True,\
                                     fields='symbol, price, cum_volume, cum_amount, cum_position, created_at')
            # 00:00 到03:00
            history_data_2 = history(symbol=symbol, frequency='tick', start_time='{0} 00:00'.format(self.trading_day_str), end_time='{0} 03:00'.format(self.trading_day_str), df=True,\
                                     fields='symbol, price, cum_volume, cum_amount, cum_position, created_at')
            # 03:00 到16:00
            history_data_3 = history(symbol=symbol, frequency='tick', start_time='{0} 03:00'.format(self.trading_day_str), end_time='{0} 16:00'.format(self.trading_day_str), df=True,\
                                     fields='symbol, price, cum_volume, cum_amount, cum_position, created_at')

            # 拼接
            history_data = pd.concat([history_data_1, history_data_2, history_data_3])
            # 去重
            history_data.drop_duplicates('created_at', inplace=True)

            # 保存
            if not history_data.empty:
                # 重命名时间字段, 转化为标准名称
                history_data.rename(columns={'created_at': 'strtime'}, inplace=True)

                with open(os.path.join(self.save_path, symbol + '.pkl'), 'wb') as fwb:
                    pickle.dump(history_data, fwb)
            else:
                print("无数据: {0}".format(symbol))
        except Exception as err:
            print("\033[0;36;41m下载tick数据出错: {0}\033[0m".format(symbol))
            traceback.print_exc()
            print(err)

    def save_one_symbol_monday(self, symbol):
        """ 保存一个合约tick数据(周一) """
        try:
            print("正在下载(周一): {0}".format(symbol))
            # 数据分3段下载, 每次只能下载33000根
            # 周五20:00 到 周六00:00
            history_data_1 = history(symbol=symbol, frequency='tick', start_time='{0} 20:00'.format(self.trading_day_friday_str), end_time='{0} 00:00'.format(self.trading_day_saturday_str), df=True,\
                                     fields='symbol, price, cum_volume, cum_amount, cum_position, created_at')
            if not history_data_1.empty:
                # 周五时间加两天到周一
                history_data_1.created_at = history_data_1.created_at.map(lambda date: date + timedelta(days=2))

            # 周六00:00 到周六03:00
            history_data_2 = history(symbol=symbol, frequency='tick', start_time='{0} 00:00'.format(self.trading_day_saturday_str), end_time='{0} 03:00'.format(self.trading_day_saturday_str), df=True,\
                                     fields='symbol, price, cum_volume, cum_amount, cum_position, created_at')
            if not history_data_2.empty:
                # 周五时间加两天到周一
                history_data_2.created_at = history_data_2.created_at.map(lambda date: date + timedelta(days=2))

            # 周一03:00 到 周一16:00
            history_data_3 = history(symbol=symbol, frequency='tick', start_time='{0} 03:00'.format(self.trading_day_monday_str), end_time='{0} 16:00'.format(self.trading_day_monday_str), df=True,\
                                     fields='symbol, price, cum_volume, cum_amount, cum_position, created_at')

            # 拼接
            history_data = pd.concat([history_data_1, history_data_2, history_data_3])
            # 去重
            history_data.drop_duplicates('created_at', inplace=True)

            # 保存
            if not history_data.empty:
                # 重命名时间字段, 转化为标准名称
                history_data.rename(columns={'created_at': 'strtime'}, inplace=True)

                with open(os.path.join(self.save_path, symbol + '.pkl'), 'wb') as fwb:
                    pickle.dump(history_data, fwb)
            else:
                print("无数据: {0}".format(symbol))
        except Exception as err:
            print("\033[0;36;41m下载tick数据出错: {0}\033[0m".format(symbol))
            traceback.print_exc()
            print(err)

    def download(self):
        """ 下载数据 """
        if self.__no_trading_flag:
            # 如果没有交易就退出
            return
        print("\033[0;36;44m正在下载tick..\033[0m")
        st = time.time()
        if self.__is_monday_flag:
            for symbol in tqdm.tqdm(self.symbol_list, desc=self.info):
                self.save_one_symbol_monday(symbol)
        else:
            for symbol in tqdm.tqdm(self.symbol_list, desc=self.info):
                self.save_one_symbol(symbol)
        print("下载完毕, 用时: \033[0;36;44m{0}\033[0m".format(time.time()-st))

def download_tick_pro_fun(token, trading_day, symbol_list, info=''):
    """ tick下载函数多进程函数 """
    gd = Gm_download_one_day(token, trading_day, symbol_list, info)
    gd.download()

def get_symbol(bytes_symbol_no_Exchange_id):
    """ 合约代码转化函数 """
    return st.get_Exchange_id(st.translation_symbol(bytes_symbol_no_Exchange_id, lower=False)) + '.' + bytes_symbol_no_Exchange_id.decode()

def download_tick(token, trading_day):
    """ 下载某个交易日的tick数据 """
    # 任务列表
    # 去除指数外总共3个文件
    # 上期所合约比较多,分两个文件
    symbol_list_1 = list(map(get_symbol, st.get_sub_list1(fix_night_symbol=False)))
    symbol_list_1_1 = symbol_list_1[:142]
    symbol_list_1_2 = symbol_list_1[142:]
    # 大商所
    symbol_list_2 = list(map(get_symbol, st.get_sub_list2(fix_night_symbol=False)))
    # 郑商所 能源中心
    symbol_list_3 = list(map(get_symbol, st.get_sub_list3(fix_night_symbol=False)))

    p1_1 = Process(target=download_tick_pro_fun, args=(token, trading_day, symbol_list_1_1, "SHFE&CFFEX_1"))
    p1_2 = Process(target=download_tick_pro_fun, args=(token, trading_day, symbol_list_1_2, "SHFE&CFFEX_2"))
    p2 = Process(target=download_tick_pro_fun, args=(token, trading_day, symbol_list_2, "DCE"))
    p3 = Process(target=download_tick_pro_fun, args=(token, trading_day, symbol_list_3, "CZCE&INE"))

    p1_1.start()
    p1_2.start()
    p2.start()
    p3.start()

    p1_1.join()
    p1_2.join()
    p2.join()
    p3.join()

if __name__=='__main__':
    """ 下载一个交易日tick数据 """
    # 掘金token
    #  token = 'fa511222c7caafd5ceb462299d4ca1444915052d'
    # 需要下载的交易日
    #  trading_day = datetime(2019, 8, 13)

    # 运行函数进行下载
    #  download_tick(token, trading_day)
