# -*- coding: utf-8 -*-
# This file is a part of DDT (https://github.com/txels/ddt)
# Copyright 2012-2015 Carles Barrobés and DDT contributors
# For the exact contribution history, see the git revision log.
# DDT is licensed under the MIT License, included in
# https://github.com/txels/ddt/blob/master/LICENSE.md

import inspect
import json
import os
import re
from functools import wraps

try:
    import yaml
except ImportError:  # pragma: no cover
    _have_yaml = False
else:
    _have_yaml = True

__version__ = '1.1.1'

# These attributes will not conflict with any real python attribute
# They are added to the decorated test method and processed later
# by the `ddt` class decorator.

DATA_ATTR = '%values'      # store the data the test must run with
FILE_ATTR = '%file_path'   # store the path to JSON file
UNPACK_ATTR = '%unpack'    # remember that we have to unpack values
index_len = 5              # default max length of case index


try:
    trivial_types = (type(None), bool, int, float, basestring)
except NameError:
    trivial_types = (type(None), bool, int, float, str)


def is_trivial(value):
    if isinstance(value, trivial_types):
        return True
    elif isinstance(value, (list, tuple)):
        return all(map(is_trivial, value))
    return False


def unpack(func):
    """
    Method decorator to add unpack feature.

    """
    setattr(func, UNPACK_ATTR, True)
    return func


def data(*values):
    """
    Method decorator to add to your test methods.

    Should be added to methods of instances of ``unittest.TestCase``.

    """
    global index_len
    index_len = len(str(len(values)))
    return idata(values)


def idata(iterable):
    """
    Method decorator to add to your test methods.

    Should be added to methods of instances of ``unittest.TestCase``.

    """
    def wrapper(func):
        setattr(func, DATA_ATTR, iterable)
        return func
    return wrapper


def file_data(value):
    """
    Method decorator to add to your test methods.

    Should be added to methods of instances of ``unittest.TestCase``.

    ``value`` should be a path relative to the directory of the file
    containing the decorated ``unittest.TestCase``. The file
    should contain JSON encoded data, that can either be a list or a
    dict.

    In case of a list, each value in the list will correspond to one
    test case, and the value will be concatenated to the test method
    name.

    In case of a dict, keys will be used as suffixes to the name of the
    test case, and values will be fed as test data.

    """
    def wrapper(func):
        setattr(func, FILE_ATTR, value)
        return func
    return wrapper


def mk_test_name(name, value, index=0):
    """
    Generate a new name for a test case.

    It will take the original test name and append an ordinal index and a
    string representation of the value, and convert the result into a valid
    python identifier by replacing extraneous characters with ``_``.

    We avoid doing str(value) if dealing with non-trivial values.
    The problem is possible different names with different runs, e.g.
    different order of dictionary keys (see PYTHONHASHSEED) or dealing
    with mock objects.
    Trivial scalar values are passed as is.

    A "trivial" value is a plain scalar, or a tuple or list consisting
    only of trivial values.
    """

    # Add zeros before index to keep order
    index = "{0:0{1}}".format(index + 1, index_len)
    if not is_trivial(value):
        return "{0}_{1}".format(name, index)
    try:
        value = str(value)
    except UnicodeEncodeError:
        # fallback for python2
        value = value.encode('ascii', 'backslashreplace')
    test_name = "{0}_{1}_{2}".format(name, index, value)
    return re.sub(r'\W|^(?=\d)', '_', test_name)


def feed_data(func, new_name, *args, **kwargs):
    """
    This internal method decorator feeds the test data item to the test.

    """
    @wraps(func)
    def wrapper(self):
        return func(self, *args, **kwargs)
    wrapper.__name__ = new_name
    wrapper.__wrapped__ = func
    # Try to call format on the docstring
    if func.__doc__:
        try:
            wrapper.__doc__ = func.__doc__.format(*args, **kwargs)
        except (IndexError, KeyError):
            # Maybe the user has added some of the formating strings
            # unintentionally in the docstring. Do not raise an exception as it
            # could be that he is not aware of the formating feature.
            pass
    return wrapper


def add_test(cls, test_name, func, *args, **kwargs):
    """
    Add a test case to this class.

    The test will be based on an existing function but will give it a new
    name.

    """
    setattr(cls, test_name, feed_data(func, test_name, *args, **kwargs))


def process_file_data(cls, name, func, file_attr):
    """
    Process the parameter in the `file_data` decorator.

    """
    cls_path = os.path.abspath(inspect.getsourcefile(cls))
    data_file_path = os.path.join(os.path.dirname(cls_path), file_attr)

    def create_error_func(message):  # pylint: disable-msg=W0613
        def func(*args):
            raise ValueError(message % file_attr)
        return func

    # If file does not exist, provide an error function instead
    if not os.path.exists(data_file_path):
        test_name = mk_test_name(name, "error")
        add_test(cls, test_name, create_error_func("%s does not exist"), None)
        return

    _is_yaml_file = data_file_path.endswith((".yml", ".yaml"))

    # Don't have YAML but want to use YAML file.
    if _is_yaml_file and not _have_yaml:
        test_name = mk_test_name(name, "error")
        add_test(
            cls,
            test_name,
            create_error_func("%s is a YAML file, please install PyYAML"),
            None
        )
        return

    with open(data_file_path) as f:
        # Load the data from YAML or JSON
        if _is_yaml_file:
            data = yaml.safe_load(f)
        else:
            data = json.load(f)

    _add_tests_from_data(cls, name, func, data)


def _add_tests_from_data(cls, name, func, data):
    """
    Add tests from data loaded from the data file into the class

    """
    for i, elem in enumerate(data):
        if isinstance(data, dict):
            key, value = elem, data[elem]
            test_name = mk_test_name(name, key, i)
        elif isinstance(data, list):
            value = elem
            test_name = mk_test_name(name, value, i)
        if isinstance(value, dict):
            add_test(cls, test_name, func, **value)
        else:
            add_test(cls, test_name, func, value)


def ddt(cls):
    """
    Class decorator for subclasses of ``unittest.TestCase``.

    Apply this decorator to the test case class, and then
    decorate test methods with ``@data``.

    For each method decorated with ``@data``, this will effectively create as
    many methods as data items are passed as parameters to ``@data``.

    The names of the test methods follow the pattern
    ``original_test_name_{ordinal}_{data}``. ``ordinal`` is the position of the
    data argument, starting with 1.

    For data we use a string representation of the data value converted into a
    valid python identifier.  If ``data.__name__`` exists, we use that instead.

    For each method decorated with ``@file_data('test_data.json')``, the
    decorator will try to load the test_data.json file located relative
    to the python file containing the method that is decorated. It will,
    for each ``test_name`` key create as many methods in the list of values
    from the ``data`` key.

    """
    for name, func in list(cls.__dict__.items()):
        if hasattr(func, DATA_ATTR):
            for i, v in enumerate(getattr(func, DATA_ATTR)):
                test_name = mk_test_name(name, getattr(v, "__name__", v), i)
                if hasattr(func, UNPACK_ATTR):
                    if isinstance(v, tuple) or isinstance(v, list):
                        add_test(cls, test_name, func, *v)
                    else:
                        # unpack dictionary
                        add_test(cls, test_name, func, **v)
                else:
                    add_test(cls, test_name, func, v)
            delattr(cls, name)
        elif hasattr(func, FILE_ATTR):
            file_attr = getattr(func, FILE_ATTR)
            process_file_data(cls, name, func, file_attr)
            delattr(cls, name)
    return cls
