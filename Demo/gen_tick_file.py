#!/use/bin/dev python
# -*- coding: utf-8 -*-
from datetime import datetime
from tick_lib.gen_tick_file import gen_tick_file

if __name__ == '__main__':
    """ 生成某个交易日的tick csv文件 """

    trading_day = datetime(2019, 8, 13)

    gcf = gen_tick_file(trading_day)

    gcf.run()
