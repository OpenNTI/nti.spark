#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import none
from hamcrest import raises
from hamcrest import calling
from hamcrest import assert_that

import time
import unittest
from datetime import datetime

from nti.spark.utils import parse_date


class TestUtils(unittest.TestCase):

    def test_parse_date(self):
        now = time.time()
        assert_that(parse_date(None), is_(none()))
        assert_that(parse_date(now), is_(datetime))
        assert_that(parse_date(int(now)), is_(datetime))
        assert_that(parse_date(str(now)), is_(datetime))
        assert_that(parse_date('19731130'), is_(datetime))
        assert_that(parse_date('1973-11-30'), is_(datetime))
        assert_that(parse_date('1973-11-30T00:00:00Z'), is_(datetime))
        assert_that(calling(parse_date).with_args('invalid_date'),
                    raises(ValueError))
