#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools
import inspect
import os.path

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

def checkcaller(caller: str = 'package'):
    # 限制函数作用域

    def gcp_package():
        currentFrame = inspect.currentframe()
        outerFrames = inspect.getouterframes(currentFrame)
        # `outerFrames[0]` 就是被调用函数，在被调用时，所在的帧 `currentFrame`
        callerFrame = outerFrames[2]
        file = callerFrame.filename
        path = os.path.abspath(file)
        return path

    get_current_path = {
        'package': gcp_package,
    }.get(caller)

    if get_current_path is None:
        raise RuntimeError(f'Invalid argument: `(caller={caller})` is unsupported!')

    def decorator_checkcaller(func):

        # 获取“声明路径”
        declare_path = get_current_path()
        print('Declare Path:', declare_path)

        @functools.wraps(func)
        def wrapper_checkcaller(*args, **kwargs):

            # 获取“调用路径”
            invoker_path = get_current_path()
            print('Invoker Path:', invoker_path)

            if invoker_path == declare_path:
                result = func(*args, **kwargs)
                return result
            else:
                raise AssertionError(f'Private function {func.__name__!r} invoked outside its declaring context!')
        return wrapper_checkcaller
    return decorator_checkcaller

package = checkcaller('package')
checkcaller = checktype(caller = str)(checkcaller)

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
