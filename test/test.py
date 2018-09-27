import sys
import sqlite3
import unittest
import datetime
import logging

import pandas as pd
import numpy as np

sys.path.append('..')

import futures

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

        shfe = futures.SHFE()
        shfe.loadTable(force_new=True)

        self.assertTrue(shfe.table is not None, 'DataFrame `shfe.table` is not properly initialized!')
        self.assertTrue(shfe.table.size == 0, 'DataFrame `shfe.table` should be EMPTY!')

        with open(uri, mode='r', encoding='utf-8') as file:
            text = file.read()
            shfe.parseText(datetime.date(2018, 9, 20), text)

        self.assertTrue(shfe.table.size > 0, 'DataFrame `shfe.table` should NOT be EMPTY!')

        logger.info(f'Complete parsing!\n------------------------------\nTable:\n{shfe.table}\n------------------------------')

        table = shfe.table.copy(deep=True)
        shfe.saveTable()

        logger.info('Double-checking!')

        shfe.loadTable()

        self.assertTrue(shfe.table is not None, 'Fail to reload dataFrame `shfe.table`!')
        pd.testing.assert_frame_equal(table,
                shfe.table,
                check_dtype=False,
            ) # 'Fail to keep load/save consistency!'

if __name__ == '__main__':
    unittest.main()
