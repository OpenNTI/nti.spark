#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import assert_that

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

from zope import component

from nti.spark.interfaces import IHiveContext
from nti.spark.interfaces import ISparkContext
from nti.spark.interfaces import ISparkSession
from nti.spark.interfaces import ISparkInstance
from nti.spark.interfaces import IHiveSparkInstance

import nti.testing.base

SPARK_ZCML_STRING = u"""
<configure xmlns="http://namespaces.zope.org/zope"
    xmlns:zcml="http://namespaces.zope.org/zcml"
    xmlns:spark="http://nextthought.com/ntp/spark"
    i18n_domain='nti.spark'>

    <include package="zope.component" />
    <include package="nti.spark" />

    <include package="." file="meta.zcml" />

    <spark:registerSparkInstance/>

</configure>
"""

HIVESPARK_ZCML_STRING = u"""
<configure xmlns="http://namespaces.zope.org/zope"
    xmlns:zcml="http://namespaces.zope.org/zcml"
    xmlns:spark="http://nextthought.com/ntp/spark"
    i18n_domain='nti.spark'>

    <include package="zope.component" />
    <include package="nti.spark" />

    <include package="." file="meta.zcml" />

    <spark:registerHiveSparkInstance app_name="HiveSpark App" />

</configure>
"""


class TestZcml(nti.testing.base.ConfiguringTestBase):

    def test_spark_registration(self):
        self.configure_string(SPARK_ZCML_STRING)
        spark = component.getUtility(ISparkInstance)
        assert_that(spark, validly_provides(ISparkInstance))
        assert_that(spark, verifiably_provides(ISparkInstance))
        assert_that(spark.context, validly_provides(ISparkContext))
        assert_that(spark.context, verifiably_provides(ISparkContext))
        assert_that(spark.session, validly_provides(ISparkSession))
        assert_that(spark.session, verifiably_provides(ISparkSession))
        spark.close()

    def test_hive_spark_registration(self):
        self.configure_string(HIVESPARK_ZCML_STRING)
        spark = component.getUtility(IHiveSparkInstance)
        assert_that(spark, validly_provides(IHiveSparkInstance))
        assert_that(spark, verifiably_provides(IHiveSparkInstance))
        assert_that(spark.hive, validly_provides(IHiveContext))
        spark.close()
