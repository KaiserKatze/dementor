#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools
import inspect
import os.path
import sys

def checktype(**kwargs):
    rules = kwargs
    # Validate rule entries
    for paramType in rules.values():
        if not isinstance(paramType, type):
            raise TypeError('Invalid decorator arguments! Subclass of `type` is needed.')

    def decorator_checktype(func):
        @functools.wraps(func)
        def wrapper_checktype(*args, **kwargs):
            # Get parameter name list
            paramNames = inspect.getargspec(func)[0]

            for paramName in rules:
                paramType = rules[paramName]

                paramValue = kwargs.get(paramName)
                if not paramValue and paramName in paramNames:
                    paramIndex = paramNames.index(paramName)
                    paramValue = args[paramIndex]

                if isinstance(paramValue, paramType):
                    continue

                raise AssertionError(f'Type of argument `{paramName}` is `{paramValue}`, violating the expectation of its being instance of `{paramType}`!')

            result = func(*args, **kwargs)

            return result
        return wrapper_checktype
    return decorator_checktype

#####################################################################

def private(func):
    def get_current_path():
        # TODO 找不到合适的函数，用以获得“声明路径”及“调用路径”
        #return os.path.abspath(sys.argv[0])
        #return os.path.abspath(__file__)
        raise RuntimeError('Unsupported!')

    declare_path = get_current_path()
    print('Declare Path:', declare_path)

    @functools.wraps(func)
    def wrapper_private(*args, **kwargs):
        invoker_path = get_current_path()
        print('Invoker Path:', invoker_path)

        if invoker_path == declare_path:
            result = func(*args, **kwargs)
            return result

        else:
            raise AssertionError(f'Private function {func.__name__!r} invoked outside its declaring context!')

    return wrapper_private

#####################################################################

if __name__ == '__main__':
    msg = 'Decorator `checktype` fails to fullfilled runtime type checking.'

    try:
        assert checktype(param = int), msg  # 0
        assert checktype(apple = str), msg  # 1

        @checktype(x = int, y = int)
        def add(x, y):
            return x + y

        assert add(1, 2) == 3, msg          # 2

        @checktype(x = int, y = int)
        def mul(x, y):
            return x * y

        assert add(5, 6) == 11, msg         # 3

    except:
        result = False
    else:
        result = True
    finally:
        assert result, msg                  # 4

    try:
        add('anything', None)

        checktype(x = 'hello')
    except:
        result = True
    else:
        result = False
    finally:
        assert result, msg                  # 5

    print('All tests passed.')
