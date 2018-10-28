#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging

from target import Target, Session
from .parsers import CurrencyParser
from .spiders import CurrencySpider

############################################################################
logger = logging.getLogger(__name__)
############################################################################

class TargetExt(Target):

    HOSTNAME = 'srh.bankofchina.com'
    URL_REFERER = None

    def __init__(self):
        super().__init__(\
                session=Session(\
                        host=self.HOSTNAME,
                        referer=self.URL_REFERER,
                    ),
            )

    def loadTable(self, force_new=False):
        super().loadTable(self.DEFAULT_COLUMNS, force_new,)

class BankOfChina(TargetExt):

    def __init__(self):
        pass
