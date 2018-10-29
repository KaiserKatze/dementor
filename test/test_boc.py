import logging
import os.path
import unittest

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

here = os.path.abspath(os.path.dirname(__file__))

from targets.boc import BankOfChina
from target import Session

LOGGING_FILE = 'test.log'
logging.basicConfig(\
    filename=LOGGING_FILE,
    filemode='w',
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
)
logger = logging.getLogger(__name__)

class TestBOC(unittest.TestCase):
    def test_io(self):
        paths = ['boc-20181024.html', 'boc-20080101.html']

        def test(path):
            print(f'Testing sample file {path!r} ...')

            reportDate = datetime.datetime.strptime(path[4:-5], '%Y%m%d')
            path = os.path.join(here, path)

            boc = BankOfChina()
            boc.loadTable(force_new=True)

            with open(path, mode='r', encoding='utf-8') as file:
                text = file.read()
                boc.parseText(reportDate, text)
                self.assertTrue(isinstance(shfe.table, pd.DataFrame),)
                self.assertTrue(shfe.table.size > 0, 'Output DataFrame `shfe.table` should NOT be EMPTY!')

if __name__ == '__main__':
    unittest.main()
