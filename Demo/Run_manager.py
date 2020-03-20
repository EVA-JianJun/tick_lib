#!/use/bin/dev python
# -*- coding: utf-8 -*-
from tick_lib.Gm_tick_manager import Gm_tick_manager

if __name__ == '__main__':
    """ tick下载管理器 """

    token = 'your token'

    gtm = Gm_tick_manager(token)

    gtm.run()
