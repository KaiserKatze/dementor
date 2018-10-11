#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import logging
import collections

import pandas as pd
import numpy as np

from target import BaseParser, SpiderException

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class TimePriceParser(BaseParser):

    PRODUCTIDS = ( 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG', 'RB', 'WR', 'HC', 'FU', 'BU', 'RU', )

    def parseChunk(self,
                reportDate: datetime.date,
                chunk,
            ):
        if not reportDate:
            raise TypeError('Argument `reportDate` is not specified!')
        if not isinstance(reportDate, datetime.date):
            raise TypeError(f'Argument `reportDate` has invalid type: `{type(reportDate)}`!')
        if not chunk:
            raise TypeError('Argument `chunk` is not specified!')

        '''合约名称'''
        sInstrumentId = chunk['INSTRUMENTID']
        instrumentId = sInstrumentId.strip().upper()
        if not instrumentId[:2] in self.PRODUCTIDS:
            raise SpiderException(f'Invalid `instrumentId`: {instrumentId}!')

        '''加权平均价'''
        sRefSettlementPrice = chunk['REFSETTLEMENTPRICE']
        refSettlementPrice = int(sRefSettlementPrice)

        '''涨跌'''
        sUpdown = chunk['UPDOWN']
        updown = int(round(float(sUpdown)))

        '''创建新记录（行）'''
        row = pd.Series([
                    instrumentId,
                    refSettlementPrice,
                    updown,
                ],
                index = self.DEFAULT_COLUMNS,
                name = pd.to_datetime(reportDate),
            )

        #logger.info(row)

        return row

    def parseText(self,
                reportDate: datetime.date,
                text: str,
            ):
        if not text:
            raise SpiderException(TypeError('Argument `text` is not specified!'))

        result = json.loads(text)
        chunks = result.get('o_currefprice')
        if not isinstance(chunks, collections.Iterable):
            raise SpiderException('Invalid data structure: invalid chunks found!')

        for chunk in chunks:
            if not isinstance(chunk, dict):
                raise SpiderException('Invalid data structure: invalid chunk found!')

            chunkTime = chunk.get('TIME')
            if not isinstance(chunkTime, str):
                raise SpiderException('Invalid data structure: invalid TIME found!')

            if '9:00-15:00' in chunkTime:
                row = self.parseChunk(reportDate, chunk)
                table = self.table
                table = table.append(row)
                self.table = table

    def parseData(self, reportDate, response):
        try:
            text = response.text
            self.parseText(reportDate, text)

        except SpiderException as e:
            sReportDate = reportDate.strftime('%Y-%m-%d')
            logger.info(f'''
                    reportDate = {sReportDate}
                    response.text =
                    {text}
                    ''')
            logger.exception(e)
