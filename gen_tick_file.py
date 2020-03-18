#!/use/bin/dev python
# -*- coding: utf-8 -*-
import os
import tqdm
import time
import pickle
import traceback
import pandas as pd
from datetime import datetime

trading_day = datetime(2019, 8, 13)

class gen_tick_file():

    def __init__(self, trading_day):
        """ 初始化 """
        # 交易日
        self.trading_day = trading_day
        # 交易日字符串
        trading_day_str = trading_day.strftime('%Y-%m-%d')

        # tick文件目录
        tick_file_path = os.path.join('download', trading_day_str)

        self.tick_file_list = list(map(lambda file_name: os.path.join(tick_file_path, file_name), os.listdir(tick_file_path)))

        # all_df
        self.all_df = pd.DataFrame()

        # 保存路径
        self.save_path = './download_tick_pkl/{0}'.format(trading_day_str)

        # 保存文件路径
        self.save_file_path = os.path.join(self.save_path, 'tick_'+trading_day_str.replace('-', '')+'.pkl')

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

    def read_file(self):
        """ 读取tick文件 """
        print("\033[0;36;44m正在生成tick文件..\033[0m")
        all_tick_df_list = []
        for tick_file in tqdm.tqdm(self.tick_file_list):
            try:
                #  print("\033[0;36;44m正在读取: {0}\033[0m".format(tick_file))
                with open(tick_file, 'rb') as fr:
                    df_tmp = pickle.load(fr)
                    all_tick_df_list.append(df_tmp)
            except Exception as err:
                print("\033[0;36;41m读取tick 文件失败: {0}\033[0m".format(tick_file))
                traceback.print_exc()
                print(err)

        # 最后在拼接到一起
        self.all_df = pd.concat(all_tick_df_list)

    def save_file(self):
        """ 保存为整个大文件 """
        try:
            st = time.time()
            print("正在生成保存tick大文件: \033[0;36;44m{0}\033[0m".format(self.save_file_path))
            with open(self.save_file_path, 'wb') as fw:
                pickle.dump(self.all_df, fw)
            print("完毕!")
        except Exception as err:
            print("\033[0;36;41m保存tick文件失败: {0}\033[0m".format(self.save_file_path))
            traceback.print_exc()
            print(err)
        finally:
            print("用时: {0}".format(time.time() - st))

    def run(self):
        """ 运行完整逻辑 """
        self.read_file()
        self.save_file()
