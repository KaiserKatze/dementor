#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import logging

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from target import BaseParser, SpiderException

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class CurrencyParser(BaseParser):

    def parseHtml(self, reportDate: datetime.date, soup):
        if getattr(self, 'pages', None) is None:
            # 总页数
            node = soup.select_one('#list_navigator li:nth-of-type(1)')
            # `tPages` 取值形如 '共5页'
            tPages = node.string
            # 提取 '共' 与 '页' 之间的数字
            sPages = tPages[1:-1]
            assert(len(sPages) > 0,
                f'Unexpected HTML structure: {tPages!r}!')
            # 强制转换类型为数字
            pages = int(sPages)
            self.pages = pages

        rows = soup.select('div.BOC_main.publish table tr')
        for rowId in range(1, len(rows)):
            row = rows[rowId]
            self.parseChunk(reportDate, row)

    def parseChunk(self, reportDate: datetime.date, chunk):
        cells = chunk.select('td')

        # 每行有 7 列：
        #   货币名称
        #   现汇买入价
        #   现钞买入价
        #   现汇卖出价
        #   现钞卖出价
        #   中行折算价
        #   发布时间

        assert(len(cells) == 7,
            f'Unexpected HTML structure: {len(cells)} cells in row!')

        # 货币名称
        cell0 = cells[0]
        sCurrencyName = cell0.string.strip()
        # 汇率
        rates = map(lambda cell: float(cell.string), cells[1:-1])
        # 发布时间
        cell6 = cells[6]
        sPublishTime = cell6.string.strip()
        publishTime = datetime.datetime.strptime(sPublishTime, '%Y.%m.%d %H:%M:%S')

        row = [sCurrencyName, *rates, publishTime]
        row = pd.Series(row,
                index=self.DEFAULT_COLUMNS,
                name=pd.to_datetime(reportDate),)

        return row
