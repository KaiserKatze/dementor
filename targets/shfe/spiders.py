#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import dateutil.rrule as rrule
import enum
import logging

import pandas as pd
import numpy as np

import handlers
import futures
import dtutil

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class BaseSpider(handlers.FutureHandler):
    pass

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
            table = self.getTable()
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

    def fetchData(self, session, reportDate, suffix):
        url = self.generateUrl(reportDate, suffix)
        if not url:
            return

        response = session.get(url)

        logger.info(f'Fetching data for `url={url}` ...')

        if response.status_code == 200:
            self.parseData(reportDate, response)

        else:
            logger.error(f'Fail to retrieve request(url={url})!')

    # public
    def traverseDate(self,
                dsrc,
                ddst,
                callback=None,
            ):
        if not isinstance(dsrc, datetime.date):
            raise TypeError(f'Argument `dsrc` has invalid type: `{type(dsrc)}`!')
        if not isinstance(ddst, datetime.date):
            raise TypeError(f'Argument `ddst` has invalid type: `{type(ddst)}`!')
        if not dsrc < ddst:
            raise ValueError('Argument `dsrc` should be earlier than `ddst`!')

        if callback is None:
            session = self.getSession()
            callback = lambda dt: self.fetchData(session, dt, TimePriceSpider.Suffix.daily)

        def TraversalTask(cb, ttdsrc, ttddst):
            for reportDate in rrule.rrule(rrule.DAILY, dtstart=ttdsrc, until=ttddst):
                cb(reportDate)

            logger.info('Complete traversal successfully!')

        tasks = []
        for year in range(dsrc.year, ddst.year):
            ndsrc = datetime.date(year, 1, 1)
            nddst = datetime.date(year, 12, 31)
            task = TraversalTask(callback, ndsrc, nddst)
            tasks.append(task)
        ndsrc = datetime.date(ddst.year, 1, 1)
        nddst = ddst
        task = TraversalTask(callback, ndsrc, nddst)
        tasks.append(task)

        executor = self.getExecutor()
        executor.submit(task)
