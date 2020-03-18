#!/use/bin/dev python
# -*- coding: utf-8 -*-
import os
import sys
import time
import traceback
from threading import Thread, Lock
from datetime import datetime, timedelta
from tick_lib.download_tick import download_tick
from tick_lib.gen_tick_file import gen_tick_file
from MyCTP.DataServer.TickFileFormat import TickFileFormat
from MyCTP.shutil_pro.shutil_pro import shutil_pro

class Gm_tick_manager():

    def __init__(self, token):
        """ 初始化 """
        self.token = token

        # 下载数据锁
        self.download_tick_lock = Lock()
        # 生成tick文件锁
        self.gen_tick_file_lock = Lock()
        # 生成h5文件锁
        self.gen_h5_tick_file_lock = Lock()

        self.sp = shutil_pro()

        # 运行自动下载tick服务
        self.auto_download_tick()

    def auto_download_tick(self):
        """ 自动运行下载tick """
        def auto_download_tick_thr():
            """ 自动下载线程 """
            while True:
                try:
                    now = datetime.now()
                    now_1600 = now.replace(hour=16, minute=0, second=0, microsecond=0)
                    if now < now_1600:
                        # 如果当前时间在1600之前
                        t = now_1600.timestamp() - now.timestamp()
                    else:
                        # 当前时间已经过了1600了
                        now_1600_plus_one_day = now_1600 + timedelta(days=1)
                        t = now_1600_plus_one_day.timestamp() - now.timestamp()

                    next_run_date = now + timedelta(seconds=t)
                    print("下次下载tick时间: \033[0;36;44m{0}\033[0m".format(next_run_date))

                    # 等待t秒后运行下载函数
                    time.sleep(t)

                    self.download_tick_lock.acquire()
                    try:
                        # 获取交易日
                        now = datetime.now()
                        trading_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

                        print("交易日: \033[0;36;44m{0}\033[0m".format(trading_day))

                        # 运行下载函数
                        download_tick(self.token, trading_day)
                    finally:
                        self.download_tick_lock.release()
                except Exception as err:
                    print("\033[0;36;41m下载tick错误!\033[0m")
                    traceback.print_exc()
                    print(err)

        download_tick_thr = Thread(target=auto_download_tick_thr, name='auto_download_tick_thr')
        download_tick_thr.setDaemon(True)
        download_tick_thr.start()

    """ 用户函数 """
    def download_tick_user(self):
        """ 用户控制下载一个交易日tick """
        trading_day = input("Please input trading_day, eg: 20190813.\ntrading_day=")

        try:
            trading_day = datetime.strptime(trading_day, '%Y%m%d')
        except ValueError:
            print("\033[0;36;42mtrading_day format err!\033[0m")
            return

        # 下载tick
        self.download_tick_lock.acquire()
        try:
            print("交易日: \033[0;36;44m{0}\033[0m".format(trading_day))
            # 运行下载函数
            download_tick(self.token, trading_day)
        except Exception as err:
            print("\033[0;36;41m下载tick错误!\033[0m")
            traceback.print_exc()
            print(err)
        finally:
            self.download_tick_lock.release()

    def gen_tick_file_user(self):
        """ 生成一个tick pkl文件 """
        trading_day = input("Please input trading_day, eg: 20190813.\ntrading_day=")

        try:
            trading_day = datetime.strptime(trading_day, '%Y%m%d')
        except ValueError:
            print("\033[0;36;42mtrading_day format err!\033[0m")
            return

        self.gen_tick_file_lock.acquire()
        try:
            print("交易日: \033[0;36;44m{0}\033[0m".format(trading_day))
            gcf = gen_tick_file(trading_day)

            gcf.run()
        except Exception as err:
            print("\033[0;36;41m生成tick pkl文件错误!\033[0m")
            traceback.print_exc()
            print(err)
        finally:
            self.gen_tick_file_lock.release()

    def gen_h5_tick_file(self):
        """ 生成h5 tick文件 """
        trading_day = input("Please input trading_day, eg: 20190813.\ntrading_day=")

        try:
            trading_day = datetime.strptime(trading_day, '%Y%m%d')
        except ValueError:
            print("\033[0;36;42mtrading_day format err!\033[0m")
            return

        self.gen_h5_tick_file_lock.acquire()
        try:
            print("交易日: \033[0;36;44m{0}\033[0m".format(trading_day))
            """ 生成全部的tick数据 """
            # 就是按原本的csv文件的格式然后指定参数
            old_file_path = os.path.join('.', 'download_tick_pkl', trading_day.strftime('%Y-%m-%d'), 'tick_{0}.pkl'.format(trading_day.strftime('%Y%m%d')))
            new_file_path = './tick_{0}.pkl'.format(trading_day.strftime('%Y%m%d'))
            # 移动文件到当前目录(懒得改下级包了)
            self.sp.movefile(old_file_path, new_file_path)

            # 然后生成h5文件
            tf = TickFileFormat(new_file_path)
            #  tf = TickFileFormat('./tick_20190709.csv')
            # 然后默认全部生成
            tf.run()

            """ 只生成指数tick数据 """
            # 虚拟文件,可以不存在,就是需要个日期参数,按照下面的这个文件名输入就可以了
            #  tf = TickFileFormat('./tick_20190709.csv')
            # 然后指定其他三个tick文件所在的位置,然后运行指数生成函数
            #  tf.run_index('.')
        except Exception as err:
            print("\033[0;36;41m生成h5 tick 文件错误!\033[0m")
            traceback.print_exc()
            print(err)
        finally:
            # 再移动回去
            self.sp.movefile(new_file_path, old_file_path)
            self.gen_h5_tick_file_lock.release()

    def run(self):
        """ 运行控制器 """
        time.sleep(2)
        while True:
            print("输出quit退出,输入cls清屏,输出run运行函数(1:下载tick, 2:生成tick文件, 3:生成h5 tick文件")
            user_input = input("λ: ")
            if user_input in ('QUIT', 'quit', 'Q', 'q'):
                # 退出
                sys.exit(0)
                return
            elif user_input in ('CLS', 'cls', 'clc', 'c', 'C'):
                # 清屏
                os.system('cls')
            elif user_input in ('RUN', 'run', 'R', 'r'):
                function_id = input("input function id: ")
                if function_id == '1':
                    self.download_tick_user()
                elif function_id == '2':
                    self.gen_tick_file_user()
                elif function_id == '3':
                    self.gen_h5_tick_file()
