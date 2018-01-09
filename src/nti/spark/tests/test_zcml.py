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

from nti.spark.interfaces import ISparkContext
from nti.spark.interfaces import ISparkSession
from nti.spark.interfaces import ISparkInstance

import nti.testing.base

ORGSYNC_SPARK_ZCML_STRING = u"""
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


class TestZcml(nti.testing.base.ConfiguringTestBase):

    def test_registration(self):
        self.configure_string(ORGSYNC_SPARK_ZCML_STRING)
        spark_instance = component.getUtility(ISparkInstance)
        assert_that(spark_instance, validly_provides(ISparkInstance))
        assert_that(spark_instance, verifiably_provides(ISparkInstance))
        assert_that(spark_instance.context, validly_provides(ISparkContext))
        assert_that(spark_instance.context, verifiably_provides(ISparkContext))
        assert_that(spark_instance.session, validly_provides(ISparkSession))
        assert_that(spark_instance.session, verifiably_provides(ISparkSession))
        spark_instance.close()
