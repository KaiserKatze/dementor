#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import dateutil.rrule as rrule
import enum
import logging

import pandas as pd
import numpy as np

from target import BaseSpider
import dtutil

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class TimePriceSpider(BaseSpider):

    @enum.unique
    class Suffix(enum.Enum):
        default = 'defaultTimePrice.dat'
        main    = 'mainTimePrice.dat'
        daily   = 'dailyTimePrice.dat'

    def generateUrl(self, reportDate, suffix):
        if not suffix:
            raise TypeError('Argument `suffix` is not specified!')

        super().__init__(reportDate)

        if isinstance(suffix, TimePriceSpider.Suffix):
            suffix = suffix.value

        if isinstance(suffix, str):
            # Example:
            # 'http://www.shfe.com.cn/data/dailydata/ck/20180920mainTimePrice.dat'
            sReportDate = reportDate.strftime('%Y%m%d')
            result = f'http://www.shfe.com.cn/data/dailydata/ck/{sReportDate}{suffix}'
            print('url=', result)
            return result

        else:
            raise TypeError(f'Argument `suffix={suffix}` is invalid!')

class StockSpider(BaseSpider):

    def generateUrl(self, reportDate):
        super().__init__(reportDate)

        # Example:
        # 'http://www.shfe.com.cn/data/dailydata/20181012dailystock.dat'
        sReportDate = reportDate.strftime('%Y%m%d')
        result = f'http://www.shfe.com.cn/data/dailydata/{sReportDate}dailystock.dat'
        print('url=', result)
        return result
