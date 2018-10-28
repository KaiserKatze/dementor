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

class CurrencySpider(BaseSpider):

    def generateUrl(self, reportDate, pjname: str, **kwargs):
        if not suffix:
            raise TypeError('Argument `pjname` is not specified!')
        if not isinstance(pjname, str):
            raise TypeError(f'Argument `pjname` has invalid type: `{type(date)}`!')

        if isinstance(suffix, TimePriceSpider.Suffix):
            suffix = suffix.value

        if isinstance(suffix, str):
            # Example:
            # http://srh.bankofchina.com/search/whpj/search.jsp?erectDate=2018-10-24&nothing=2018-10-24&pjname=1316
            sReportDate = reportDate.strftime('%Y%m%d')
            result = 'http://srh.bankofchina.com/search/whpj/search.jsp'
            result += f'?erectDate={sReportDate}&nothing={sReportDate}&pjname={pjname}'
            return False, result

        else:
            raise TypeError(f'Argument `suffix={suffix}` is invalid!')
