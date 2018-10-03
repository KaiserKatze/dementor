#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging

import pandas as pd
import numpy as np

'''
@see https://docs.python.org/3/library/datetime.html
@see https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Timestamp.html
'''
class MonthDay:
    def __init__(self, month, day, duration=None):
        self.month = month
        self.day = day
        self.duration = duration

    def __eq__(self, date):
        if not date:
            raise TypeError('Argument `date` is not specified!')
        if isinstance(date, datetime.date) \
                or isinstance(date, datetime.datetime) \
                or isinstance(date, pd.Timestamp):
            return self.month == date.month \
                    and (self.day == date.day \
                    or (self.duration and date.day in self.getDuration()))
        else:
            raise TypeError(f'Argument `date` has invalid type: `{type(date)}`!')

    def getDuration(self):
        return range(self.day + 1, self.day + self.duration)

'''
公历新年（元旦），放假1天（1月1日）；
春节，放假3天（农历正月初一、初二、初三）；
清明节，放假1天（阳历清明当日）；
劳动节，放假1天（5月1日）；
端午节，放假1天（农历端午当日）；
中秋节，放假1天（农历中秋当日）；
国庆节，放假3天（10月1日、2日、3日）。

@see https://zh.wikipedia.org/wiki/中华人民共和国节日与公众假期
'''
Holiday = dict(
    # 元旦节
    NEW_YEAR = MonthDay(1, 1),
    # 劳动节
    LABORS_DAY = MonthDay(5, 1),
    # 国庆节
    NATION_DAY = MonthDay(10, 1, 3),
)

def isWeekend(date):
    weekday = date.weekday()
    return weekday > 4

def isHoliday(date):
    for holiday in Holiday.values():
        if holiday == date:
            return True
    return False
