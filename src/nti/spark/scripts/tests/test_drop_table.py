#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,no-member

import os
from unittest import TestCase

import fudge

from nti.spark.scripts.drop_table import process_args
from nti.spark.scripts.drop_table import main as sync_main

from nti.spark.scripts.tests import BaseTestMixin


class TesDropTable(BaseTestMixin, TestCase):

    @fudge.patch('nti.spark.scripts.drop_table.process_args')
    def test_main(self, mock_pa):
        mock_pa.expects_call().returns_fake()
        sync_main()

    @fudge.patch('nti.spark.spark.HiveSparkInstance.drop_table')
    def test_process_args(self, mock_drop):
        mock_drop.is_callable().returns_fake()
        directory = os.path.dirname(__file__)
        args = "table -d %s" % directory
        process_args(args.split())
