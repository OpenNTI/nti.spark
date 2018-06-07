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
from datetime import date
from datetime import datetime

from nti.spark.utils import csv_mode
from nti.spark.utils import parse_date
from nti.spark.utils import safe_header
from nti.spark.utils import get_timestamp
from nti.spark.utils import parse_date_as_utc

from nti.spark.tests import SparkLayerTest


class TestUtils(SparkLayerTest):

    def test_csv_mode(self):
        assert_that(csv_mode(), is_("DROPMALFORMED"))
        assert_that(csv_mode(True), is_('FAILFAST'))

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

    def test_parse_date_as_utz(self):
        assert_that(parse_date_as_utc('19731130'), is_(datetime))
        
    def test_get_timestamp(self):
        assert_that(get_timestamp(None), is_(int))
        assert_that(get_timestamp(date.today()), is_(int))
        assert_that(get_timestamp(datetime.today()), is_(int))

    def test_safe_header(self):
        has_slash = "OU/TX Weekend"
        has_spaces = "Presidents Associates Honored"
        has_dash = "Presidents_Associates_Young_31-39_Years_Old"
        assert_that(safe_header(has_slash), "OU_TX_Weekend")
        assert_that(safe_header(has_spaces), "Presidents_Associates_Honored")
        assert_that(safe_header(has_dash), "Presidents_Associates_Young_31_39_Years_Old")
