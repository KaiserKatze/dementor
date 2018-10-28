#!/usr/bin/env python
# -*- coding: utf-8 -*-

#==========================================================================
# This file is used to setup environment for test suites in this directory.
#==========================================================================

import sys
from os.path import dirname as d, abspath as ap, join as j

src_dir = j(ap(d(d(__file__))), 'src')
sys.path.append(src_dir)
