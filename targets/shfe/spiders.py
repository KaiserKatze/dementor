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
        if not reportDate:
            raise TypeError('Argument `reportDate` is not specified!')
        if not isinstance(reportDate, datetime.date):
            raise TypeError(f'Argument `reportDate` has invalid type: `{type(reportDate)}`!')
        if not suffix:
            raise TypeError('Argument `suffix` is not specified!')

        try:
            '''假期不开展业务'''
            if dtutil.isWeekend(reportDate) or dtutil.isHoliday(reportDate):
                sReportDate = reportDate.strftime('%Y-%m-%d')
                logger.info(f'Skip ({sReportDate}) due to weekend/holiday!')
                return None

            '''检查数据库中是否已经有本日记录'''
            table = self.table
            pdReportDate = pd.to_datetime(reportDate)
            try:
                row = table.loc[pdReportDate]

            except KeyError:
                pass

            else:
                sReportDate = reportDate.strftime('%Y-%m-%d')
                logger.info(f'Skip ({sReportDate}) due to existing document!\n{row}')
                return None

        except:
            pass

        #logger.info(f'Input parameters:\ndate={reportDate}\nsuffix={suffix.value}\n')

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
        if not reportDate:
            raise TypeError('Argument `reportDate` is not specified!')
        if not isinstance(reportDate, datetime.date):
            raise TypeError(f'Argument `reportDate` has invalid type: `{type(reportDate)}`!')

        try:
            '''假期不开展业务'''
            if dtutil.isWeekend(reportDate) or dtutil.isHoliday(reportDate):
                sReportDate = reportDate.strftime('%Y-%m-%d')
                logger.info(f'Skip ({sReportDate}) due to weekend/holiday!')
                return None

            '''检查数据库中是否已经有本日记录'''
            table = self.table
            pdReportDate = pd.to_datetime(reportDate)
            try:
                row = table.loc[pdReportDate]
            except KeyError:
                pass
            else:
                sReportDate = reportDate.strftime('%Y-%m-%d')
                logger.info(f'Skip ({sReportDate}) due to existing document!\n{row}')
                return None
        except:
            pass

        # Example:
        # 'http://www.shfe.com.cn/data/dailydata/20181012dailystock.dat'
        sReportDate = reportDate.strftime('%Y%m%d')
        result = f'http://www.shfe.com.cn/data/dailydata/{sReportDate}dailystock.dat'
        print('url=', result)
        return result
