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
        # div.BOC_main.publish table
        soup.select_one('form#historysearchform')

    def parseChunk(self, reportDate: datetime.date, chunk):
        # 品种
        # 形如('铜$$COPPER')
        e_varname = chunk['VARNAME']
        e_varname = e_varname[:e_varname.index('$$')]

        # 地区
        # 形如('上海$$Shanghai')
        e_regname = chunk['REGNAME']
        try:
            index = e_regname.index('$$')
        except ValueError:
            pass
        else:
            e_regname = e_regname[:index]

        # 仓库
        # 形如('期晟公司$$Qisheng')
        e_whabbrname = chunk['WHABBRNAME']
        if 'Total' in e_whabbrname or 'Subtotal' in e_whabbrname:
            #logger.info(f'Skip ({e_whabbrname!r}) ...')
            pass
        try:
            index = e_whabbrname.index('$$')
        except ValueError:
            pass
        else:
            e_whabbrname = e_whabbrname[:index]

        # 期货
        # 形如(326)
        e_wrtwghts = chunk['WRTWGHTS']
        #print(f'e_wrtwghts = {e_wrtwghts!r}')

        # 增减
        # 形如(0)
        e_wrtchange = chunk['WRTCHANGE']
        #print(f'e_wrtchange = {e_wrtchange!r}')

        row = [
            e_varname,
            e_regname,
            e_whabbrname,
            e_wrtwghts,
            e_wrtchange,
        ]
        row = pd.Series(row,
                index=self.DEFAULT_COLUMNS,
                name=pd.to_datetime(reportDate),)

        return row
