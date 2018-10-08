#!/usr/bin/env python
# -*- coding: utf-8 -*-

class FutureHandler:

    def __init__(self, parent):
        self.parent = parent

    def getSession(self):
        return self.parent.session

    def getExecutor(self):
        return self.parent.executor

    def getTable(self):
        return self.parent.table

    def setTable(self, table):
        self.parent.table = table
