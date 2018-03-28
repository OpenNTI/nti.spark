#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,inherit-non-class

from hamcrest import is_
from hamcrest import assert_that

import unittest

from nti.spark.hive import get_timestamp


class TestHive(unittest.TestCase):

    def test_get_timestamp(self):
        assert_that(get_timestamp(), is_(int))
