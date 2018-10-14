#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from futures import SHFE

LOGGING_FILE = 'dataviewer.log'
logging.basicConfig(\
        filename = LOGGING_FILE,
        filemode = 'w',
        level = logging.INFO,
        format = '[%(asctime)s] %(levelname)s %(message)s',
        datefmt = '%m/%d/%Y %I:%M:%S %p',
    )
logger = logging.getLogger(__name__)
DEFAULT_SEPERATOR = '------------------------------'




if __name__ == '__main__':
    shfe = SHFE()

    shfe.loadTable()

    assert shfe.table.size > 0, 'DataFrame `shfe.table` should NOT be EMPTY!'

    # Example:
    # 'http://www.shfe.com.cn/data/dailydata/ck/20180102dailyTimePrice.dat'
    date = datetime.date(2018, 1, 2)
    rows = shfe.getData(date = date)

    print(rows)
