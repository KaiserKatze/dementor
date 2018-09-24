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

        shfe.loadTable()

        self.assertTrue(shfe.table is not None, 'DataFrame `shfe.table` not properly initialized!')

        with open(uri, mode='r', encoding='utf-8') as file:
            text = file.read()
            shfe.parseText(datetime.date(2018, 9, 20), text)

        self.assertTrue(shfe.table.size > 0, 'Empty DataFrame `shfe.table`!')

        logger.info(shfe.table)

        shfe.saveTable()

if __name__ == '__main__':
    unittest.main()
