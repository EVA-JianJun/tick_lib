#!/use/bin/dev python
# -*- coding: utf-8 -*-
from datetime import datetime
from tick_lib.download_tick import download_tick

if __name__ == '__main__':
    """ 下载某个交易日的tick """

    token = 'your token'
    trading_day = datetime(2019, 8, 13)

    download_tick(token, trading_day)
