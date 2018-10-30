#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging

from target import Target, Session
from .parsers import TimePriceParser, StockParser
from .spiders import TimePriceSpider, StockSpider

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class TargetExt(Target):

    HOSTNAME = 'www.shfe.com.cn'
    URL_REFERER = 'http://www.shfe.com.cn/statements/dataview.html?paramid=kx'

    def loadTable(self, force_new=False):
        super().loadTable(self.DEFAULT_COLUMNS, force_new,)

class TimePrice(TargetExt, TimePriceParser, TimePriceSpider):

    DEFAULT_COLUMNS = ( 'instrumentId', 'refSettlementPrice', 'updown', )

    def startSpider(self,
                dsrc: datetime.date=datetime.date(2013, 12, 1),
                ddst: datetime.date=datetime.date.today(),
            ):
        sdsrc = dsrc.strftime('%Y-%m-%d')
        sddst = ddst.strftime('%Y-%m-%d')
        print(f'SHFE TimePrice spider starts from `{sdsrc}` to `{sddst}` ...')

        try:
            self.loadTable()

            suffix = TimePriceSpider.Suffix.daily
            callback = lambda dt: self.fetchData(dt, suffix=suffix)
            self.traverseDate(\
                    dsrc=dsrc,
                    ddst=ddst,
                    callback=callback,
                )

        finally:
            self.saveTable()

class Stock(TargetExt, StockParser, StockSpider):

    DEFAULT_COLUMNS = (
        # 品种
        'varname',
        # 地区
        'regname',
        # 仓库
        'whabbrname',
        # 期货
        'wrtwghts',
        # 增减
        'wrtchange',
    )

    def startSpider(self,
                dsrc: datetime.date=datetime.date(2013, 12, 1),
                ddst: datetime.date=datetime.date.today(),
            ):
        sdsrc = dsrc.strftime('%Y-%m-%d')
        sddst = ddst.strftime('%Y-%m-%d')
        print(f'SHFE Stock spider starts from `{sdsrc}` to `{sddst}` ...')

        try:
            self.loadTable()

            callback = lambda dt: self.fetchData(dt)
            self.traverseDate(\
                    dsrc=dsrc,
                    ddst=ddst,
                    callback=callback,
                )

        finally:
            self.saveTable()

if __name__ == '__main__':
    shfe = TimePrice()

    import sys
    import signal

    def handler(signal, frame):
        shfe.saveTable()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    try:
        shfe.startSpider(dsrc=datetime.date(2018, 1, 1), ddst=datetime.date(2018, 1, 2))

        shfe.startSpider()
    finally:
        shfe.saveTable()

    print('All tests passed.')
