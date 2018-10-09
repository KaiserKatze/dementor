#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging

import matplotlib.pyplot as plt

from target import Target, Session
from parsers import TimePriceParser
from spiders import TimePriceSpider

############################################################################
logger = logging.getLogger(__name__)
############################################################################

def plot(dataframe, instrumentId):
    df[df['instrumentId'] == instrumentId].plot(\
            x = 'reportDate',
            y = 'refSettlementPrice',
            kind = 'line',
            title = instrumentId,
        )
    plt.show()

class SHFE(Target, TimePriceParser, TimePriceSpider):

    HOSTNAME = 'www.shfe.com.cn'
    URL_REFERER = 'http://www.shfe.com.cn/statements/dataview.html?paramid=delaymarket_cu'
    DEFAULT_COLUMNS = ( 'instrumentId', 'refSettlementPrice', 'updown', )

    def __init__(self):
        super(Target, self).__init__(\
                session = Session(\
                        host = self.HOSTNAME,
                        referer = self.URL_REFERER,
                    ),
            )

    def loadTable(self,
            force_new = False,
        ):
        super().loadTable(self.DEFAULT_COLUMNS, force_new,)

    def startSpider(self,
                dsrc: datetime.date = datetime.date(2013, 12, 1),
                ddst: datetime.date = datetime.date.today(),
            ):
        sdsrc = dsrc.strftime('%Y-%m-%d')
        sddst = ddst.strftime('%Y-%m-%d')
        print(f'SHFE spider starts from `{sdsrc}` to `{sddst}` ...')

        try:
            self.loadTable()
            self.traverseDate(\
                    dsrc = dsrc,
                    ddst = ddst,
                )
        finally:
            self.saveTable()

if __name__ == '__main__':
    shfe = SHFE()

    import sys
    import signal

    def handler(signal, frame):
        shfe.saveTable()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    try:
        shfe.startSpider(dsrc = datetime.date(2018, 1, 1), ddst = datetime.date(2018, 1, 2))

        shfe.startSpider()
    finally:
        shfe.saveTable()

    print('All tests passed.')
