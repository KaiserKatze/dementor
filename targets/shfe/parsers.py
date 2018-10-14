#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import logging

import pandas as pd
import numpy as np

from target import BaseParser, SpiderException

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class TimePriceParser(BaseParser):

    PRODUCTIDS = ( 'CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'AU', 'AG', 'RB', 'WR', 'HC', 'FU', 'BU', 'RU', )

    def parseChunk(self, reportDate: datetime.date, chunk):

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
        row = [
            instrumentId,
            refSettlementPrice,
            updown,
        ]
        row = pd.Series(row,
                index=self.DEFAULT_COLUMNS,
                name=pd.to_datetime(reportDate),
            )

        return row

    def parseJson(self, reportDate, data):
        chunks = data.get('o_currefprice')
        if not isinstance(chunks, list):
            raise SpiderException('Invalid data structure: invalid chunks found!')

        for chunk in chunks:
            if not isinstance(chunk, dict):
                raise SpiderException('Invalid data structure: invalid chunk found!')

            chunkTime = chunk.get('TIME')
            if not isinstance(chunkTime, str):
                raise SpiderException(f'Invalid data structure: invalid TIME={chunkTime!r} found!')

            #print(f'ChunkTime={chunkTime!r}')

            if '9:00-15:00' in chunkTime:
                row = self.parseChunk(reportDate, chunk)
                table = self.table
                table = table.append(row)
                self.table = table

            elif '9:00-10:15' in chunkTime:
                continue

            else:
                raise SpiderException(f'Invalid data structure: invalid TIME={chunkTime!r} found!')

class StockParser(BaseParser):

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

    def parseJson(self, reportDate, data):
        # 交易日期
        o_tradingday = data['o_tradingday']
        o_tradingday = datetime.datetime.strptime(o_tradingday, '%Y%m%d')
        # 年度期数
        o_issueno = data['o_issueno']
        o_issueno = int(o_issueno)
        # 总期数
        o_totalissueno = data['o_totalissueno']
        o_totalissueno = int(o_totalissueno)
        # 数据
        o_cursor = data['o_cursor']
        if not isinstance(o_cursor, list):
            raise SpiderException('Invalid data structure: invalid chunks found!')

        for chunk in o_cursor:
            if not isinstance(chunk, dict):
                raise SpiderException('Invalid data structure: invalid chunk found!')

            row = self.parseChunk(reportDate, chunk)
            self.table = self.table.append(row, ignore_index=True)
