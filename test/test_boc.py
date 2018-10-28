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
        path = 'boc-20181024.html'
        path = os.path.join(here, path)
        with open(path, mode='r', encoding='utf-8') as file:
            text = file.read()
            soup = BeautifulSoup(text, 'html.parser')

            # get page number
            node = soup.select('#list_navigator li:nth-of-type(1)')[0]
            pages = node.string
            pages = pages[1:-1]
            pages = int(pages)
            print('Pages:', pages)

            # scan table
            rows = soup.select('div.BOC_main.publish table tr')
            for rowId in range(1, len(rows)):
                row = rows[rowId]
                cells = row.select('td')
                print(*[cell.string for cell in cells])

if __name__ == '__main__':
    unittest.main()
