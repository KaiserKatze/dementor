#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging

from target import Target, Session
from .parsers import CurrencyParser
from .spiders import CurrencySpider

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class TargetExt(Target):

    HOSTNAME = 'srh.bankofchina.com'
    URL_REFERER = None

    def loadTable(self, force_new=False):
        super().loadTable(self.DEFAULT_COLUMNS, force_new,)

class BankOfChina(TargetExt, CurrencyParser, CurrencySpider):

    # ppr: price of purchasing remit    现汇买入价
    # ppc: price of purchasing cash     现钞买入价
    # psr: price of selling remit       现汇卖出价
    # psc: price of selling cash        现钞卖出价
    # pag: price of average             中间价
    DEFAULT_COLUMNS = ( 'currency', 'ppr', 'ppc', 'psr', 'psc', 'pag', 'time', )

    def startSpider(self,
                dsrc: datetime.date=datetime.date(2008, 1, 1),
                ddst: datetime.date=datetime.date.today(),
            ):
        sdsrc = dsrc.strftime('%Y-%m-%d')
        sddst = ddst.strftime('%Y-%m-%d')
        print(f'BOC spider starts from `{sdsrc}` to `{sddst}` ...')

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
