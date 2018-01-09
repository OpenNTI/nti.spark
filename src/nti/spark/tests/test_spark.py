#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,inherit-non-class

from hamcrest import is_
from hamcrest import raises
from hamcrest import calling
from hamcrest import assert_that

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

from zope import interface

from zope.schema import Int
from zope.schema import TextLine

from nti.spark.interfaces import IHiveContext
from nti.spark.interfaces import ISparkContext
from nti.spark.interfaces import ISparkSession
from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.schema import to_pyspark_schema

from nti.spark.spark import HiveSparkInstance

from nti.spark.tests import SparkLayerTest


class ICategory(interface.Interface):
    id = Int(title=u"The id")
    name = TextLine(title=u"The name")
category_schema = to_pyspark_schema(ICategory)


class TestSpark(SparkLayerTest):

    def spark(self):
        result = HiveSparkInstance(master=u"local", 
                                   app_name=u"HiveApp", 
                                   log_level=u"FATAL",
                                   location="")
        return result
    
    def test_hive(self):
        # from IPython.terminal.debugger import set_trace;set_trace()
        try:
            spark = self.spark()
            # 1. Verify and validate
            assert_that(spark, validly_provides(IHiveSparkInstance))
            assert_that(spark, verifiably_provides(IHiveSparkInstance))
            # context
            assert_that(spark.context, validly_provides(ISparkContext))
            assert_that(spark.context, verifiably_provides(ISparkContext))
            # session
            assert_that(spark.session, validly_provides(ISparkSession))
            assert_that(spark.session, verifiably_provides(ISparkSession))
            # hive
            assert_that(spark.hive, validly_provides(IHiveContext))
            assert_that(spark.hive, verifiably_provides(IHiveContext))
            
            # 2. create initial table
            spark.create_table("categories",
                               columns={"id": "INT",
                                        "name": "STRING"})
            # validate duplicates
            assert_that(calling(spark.create_table).with_args("categories",
                                                              columns={"id": "INT",
                                                                       "name": "STRING"},
                                                              partition_by={"name":"string"}),
                        raises(ValueError))

            spark.create_table("categories",
                               columns={"id": "INT",
                                        "name": "STRING"})
                                
            cols = [{'name': 'students', 'id': 1}]
            
            # 3. Insert data
            # pylint: disable=no-member
            source = spark.hive.createDataFrame(cols, schema=category_schema)
            # insert into empty table
            spark.insert_into("categories", source, False)
            # test overwrite
            spark.insert_into("categories", source, True)
            
            # 3. select from table
            df = spark.select_from("categories", ('name',))
            assert_that(df.count(), is_(1))
            
            # 4. create a simple like table
            spark.create_table("groups", like="categories")
            
            # 5. create table with partition
            spark.create_table("historical_categories", 
                               like="categories", 
                               partition_by={"tstamp":"double"}, 
                               external=True)
        finally:
            spark.close()
