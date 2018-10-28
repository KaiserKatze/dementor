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

if __name__ == '__main__':
    unittest.main()
