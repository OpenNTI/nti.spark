#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,inherit-non-class

from hamcrest import is_
from hamcrest import assert_that

from zope import interface

from zope.schema import Int
from zope.schema import TextLine

from nti.spark.schema import to_pyspark_schema

from nti.spark.spark import HiveSparkInstance

from nti.spark.tests import SparkLayerTest


class ICategory(interface.Interface):
    id = Int(title=u"The id")
    name = TextLine(title=u"The name")
category_schema = to_pyspark_schema(ICategory)


class TestSpark(SparkLayerTest):

    def test_hive(self):
        spark = HiveSparkInstance(appName=u"TestHiveSparkApp",
                                  log_level=u"FATAL")
        spark.create_table("categories",
                           columns={"id": "INT",
                                    "name": "STRING"})
        cols = [{'name': 'students', 'id': 1}]
        # pylint: disable=no-member
        source = spark.hive.createDataFrame(cols, schema=category_schema)
        # insert into empty table
        spark.insert_into("categories", source, False)
        # test overwrite
        spark.insert_into("categories", source, True)
        # select names
        # from IPython.terminal.debugger import set_trace;set_trace()
        df = spark.select_from("categories", ('name',))
        assert_that(df.count(), is_(1))
        # create a hive
        
        # hive.create_table("orgsync.historical_categories", like="orgsync.categories", partition_by={"tstamp":"double"}, external=True)
