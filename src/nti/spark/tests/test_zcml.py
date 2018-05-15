#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import none
from hamcrest import is_not
from hamcrest import assert_that

import os
import shutil

from zope import component

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.tests import ITestTable

import nti.testing.base

HIVESPARK_ZCML_STRING = u"""
<configure xmlns="http://namespaces.zope.org/zope"
    xmlns:zcml="http://namespaces.zope.org/zcml"
    xmlns:spark="http://nextthought.com/ntp/spark"
    i18n_domain='nti.spark'>

    <include package="zope.component" />
    <include package="nti.spark" />

    <include package="." file="meta.zcml" />

    <spark:registerHiveSparkInstance app_name="HiveSpark App" />

    <spark:registerHiveTable factory=".tests.TestTable"
                             provides=".tests.ITestTable" />

</configure>
"""


class TestZcml(nti.testing.base.ConfiguringTestBase):

    def tearDown(self):
        nti.testing.base.ConfiguringTestBase.tearDown(self)
        shutil.rmtree(os.path.join(os.getcwd(), 'home'), True)
        shutil.rmtree(os.path.join(os.getcwd(), 'metastore_db'), True)
        shutil.rmtree(os.path.join(os.getcwd(), 'spark-warehouse'), True)

    def test_registrations(self):
        self.configure_string(HIVESPARK_ZCML_STRING)
        # test spark
        spark = component.queryUtility(IHiveSparkInstance)
        assert_that(spark, is_not(none()))
        spark.close()
        # test table
        table = component.queryUtility(ITestTable)
        assert_that(table, is_not(none()))
        table = component.queryUtility(IHiveTable, "test_table")
        assert_that(table, is_not(none()))
