#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,no-member

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import raises
from hamcrest import calling
from hamcrest import assert_that

import os
import unittest
from numbers import Number

from nti.spark.scripts import get_timestamp
from nti.spark.scripts import create_context
from nti.spark.scripts import configure_logging

from nti.spark.scripts.tests import BaseTestMixin


class TestScripts(BaseTestMixin, unittest.TestCase):

    def test_invalid_dir(self):
        assert_that(calling(create_context).with_args('/tmp/__not_valid_dir__'),
                    raises(ValueError))

    def test_configure_logging(self):
        configure_logging()

    def test_get_timestamp(self):
        assert_that(get_timestamp(), is_(none()))
        assert_that(get_timestamp(current=True), is_(Number))
        assert_that(get_timestamp(source=__file__), is_(Number))

    def test_create_context(self):
        env_dir = os.path.dirname(__file__)
        assert_that(create_context(env_dir),
                    is_not(none()))
