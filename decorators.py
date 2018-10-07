#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools

def checktype(**decorator_kwargs):
    print(**decorator_kwargs)
    def decorator_checktype(func):
        @functools.wraps(func)
        def wrapper_checktype(*args, **kwargs):
            print(kwargs)
            result = func(*args, **kwargs)
            return result
        return wrapper_checktype
    return decorator_checktype
