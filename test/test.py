import sys
import sqlite3
import unittest
import datetime
import logging
import json

import pandas as pd
import numpy as np

sys.path.append('..')

from targets.shfe.futures import SHFE

LOGGING_FILE = 'futures.log'
logging.basicConfig(\
        filename=LOGGING_FILE,
        filemode='w',
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
    )
logger = logging.getLogger(__name__)

class TestSHFE(unittest.TestCase):

    def test_all(self):
        #uri = './20180920dailyTimePrice.dat'
        uri = './20180920defaultTimePrice.dat'

        shfe = SHFE()
        shfe.loadTable(force_new=True)

        #############################################
        # Check DataFrame Initialization
        #############################################

        self.assertTrue(shfe.table is not None, 'DataFrame `shfe.table` is not properly initialized!')
        self.assertTrue(shfe.table.size == 0, 'DataFrame `shfe.table` should be EMPTY!')

        with open(uri, mode='r', encoding='utf-8') as file:
            text = file.read()
            shfe.parseText(datetime.date(2018, 9, 20), text)

        #############################################
        # Check Data Interpretation
        #############################################

        self.assertTrue(shfe.table.size > 0, 'DataFrame `shfe.table` should NOT be EMPTY!')

        logger.info(f'Complete parsing!\n------------------------------\nTable:\n{shfe.table}\n------------------------------')

        table = shfe.table.copy(deep=True)
        shfe.saveTable()

        #############################################
        # Check Load/Save Consistency
        #############################################

        logger.info('Double-checking!')

        shfe.loadTable()

        self.assertTrue(shfe.table is not None, 'Fail to reload dataFrame `shfe.table`!')
        pd.testing.assert_frame_equal(table,
                shfe.table,
                check_dtype=False,
            ) # 'Fail to keep load/save consistency!'

    def test_stock(self):
        path = '20181009dailystock.dat.txt'
        file = open(path, mode = 'r', encoding = 'utf-8')
        text = file.read()
        data = json.loads(text)

        # 交易日期
        o_tradingday = data['o_tradingday']
        o_tradingday = datetime.datetime.strptime(o_tradingday, '%Y%m%d')
        self.assertTrue(o_tradingday.year == 2018)
        self.assertTrue(o_tradingday.month == 10)
        self.assertTrue(o_tradingday.day == 9)
        # 年度期数 —— 2018年第185期(总第 2436 期)
        # 返回 185
        o_issueno = data['o_issueno']
        o_issueno = int(o_issueno)
        self.assertTrue(o_issueno == 185)
        # 年度期数 —— 2018年第185期(总第 2436 期)
        o_totalissueno = data['o_totalissueno']
        o_totalissueno = int(o_totalissueno)
        self.assertTrue(o_totalissueno == 2436)
        # 数据
        o_cursor = data['o_cursor']

        columns = [
            '品种',
            '地区',
            '仓库',
            '期货',
            '增减',
        ]

        df = pd.DataFrame(\
                columns = columns,
                dtype = np.int64,
            )
        for entry in o_cursor:
            # 品种
            # 形如('铜$$COPPER')
            e_varname = entry['VARNAME']
            # 形如(0)
            e_varsoft = entry['VARSOFT']
            # 地区
            # 形如('上海$$Shanghai')
            e_regname = entry['REGNAME']
            # 形如(0)
            e_regsoft = entry['REGSOFT']
            # 仓库
            # 形如('期晟公司$$Qisheng')
            e_whabbrname = entry['WHABBRNAME']

            if 'Total' in e_whabbrname or 'Subtotal' in e_whabbrname:
                pass

            # 形如(14)
            e_whrows = entry['WHROWS']
            # 形如('2')
            e_wghtunit = entry['WGHTUNIT']
            # 期货
            # 形如(326)
            e_wrtwghts = entry['WRTWGHTS']
            # 增减
            # 形如(0)
            e_wrtchange = entry['WRTCHANGE']
            # 形如(15)
            e_roworder = entry['ROWORDER']
            # 形如('0')
            e_rowstatus = entry['ROWSTATUS']

            row = pd.Series([e_varname, e_regname, e_whabbrname, e_wrtwghts, e_wrtchange,], index = columns)
            df = df.append(row, ignore_index = True)

if __name__ == '__main__':
    unittest.main()
