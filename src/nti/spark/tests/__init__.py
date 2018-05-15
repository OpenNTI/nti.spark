#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,arguments-differ,inherit-non-class

import os
import shutil
import tempfile

from zope import component
from zope import interface

from zope.component.hooks import setHooks

from nti.testing.layers import GCLayerMixin
from nti.testing.layers import ZopeComponentLayer
from nti.testing.layers import ConfiguringLayerMixin

import zope.testing.cleanup

from nti.spark.interfaces import IHiveSparkInstance


class SharedConfiguringTestLayer(ZopeComponentLayer,
                                 GCLayerMixin,
                                 ConfiguringLayerMixin):

    set_up_packages = ('nti.spark',)

    @classmethod
    def clean_up(cls):
        shutil.rmtree(os.path.join(os.getcwd(), 'home'), True)
        shutil.rmtree(os.path.join(os.getcwd(), 'metastore_db'), True)
        shutil.rmtree(os.path.join(os.getcwd(), 'spark-warehouse'), True)

    @classmethod
    def setUp(cls):
        setHooks()
        cls.setUpPackages()

    @classmethod
    def tearDown(cls):
        component.getUtility(IHiveSparkInstance).close()
        cls.tearDownPackages()
        zope.testing.cleanup.cleanUp()
        cls.clean_up()

    @classmethod
    def testSetUp(cls, unused_test=None):
        setHooks()

    @classmethod
    def testTearDown(cls):
        pass


import unittest


class SparkLayerTest(unittest.TestCase):

    layer = SharedConfiguringTestLayer


class ITestTable(interface.Interface):
    pass

@interface.implementer(ITestTable)
class TestTable(object):
    table_name = 'test_table'
