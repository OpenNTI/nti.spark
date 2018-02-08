#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,inherit-non-class

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import raises
from hamcrest import calling
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_entries
from hamcrest import has_property

from nti.testing.matchers import validly_provides
from nti.testing.matchers import verifiably_provides

from collections import OrderedDict

from pyspark.sql.types import LongType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField

from zope import component
from zope import interface

from zope.schema import Int
from zope.schema import List
from zope.schema import TextLine

from nti.spark.hive import HiveTimeIndexed
from nti.spark.hive import HiveTimeIndexedHistoric

from nti.spark.hive import overwrite_table
from nti.spark.hive import write_to_historical

from nti.spark.interfaces import IHiveContext
from nti.spark.interfaces import ISparkContext
from nti.spark.interfaces import ISparkSession
from nti.spark.interfaces import IHiveTimeIndexed
from nti.spark.interfaces import IHiveSparkInstance
from nti.spark.interfaces import IHiveTimeIndexedHistoric

from nti.spark.schema import to_pyspark_schema

from nti.spark.tests import SparkLayerTest


class ICategory(interface.Interface):
    id = Int(title=u"The id")
    name = TextLine(title=u"The name")


class IGroup(interface.Interface):
    id = Int(title=u"The id")
    name = TextLine(title=u"The name")
    accounts = List(title=u"The id(s) of the accounts in the group",
                    min_length=0,
                    required=False,
                    value_type=Int(title=u"The id"))


groups_schema = to_pyspark_schema(IGroup)
category_schema = to_pyspark_schema(ICategory)


class TestSpark(SparkLayerTest):

    database = "db"
    table_name = "db.sample"
    historic_name = "db.historic"

    def spark(self):
        return component.getUtility(IHiveSparkInstance)

    @property
    def schema(self):
        return StructType([StructField("data", LongType(), True)])

    def check_hive_table(self, spark):
        sample_list = [(118465,), (118300,)]
        result_rdd = spark.context.parallelize(sample_list)
        result_frame = spark.hive.createDataFrame(result_rdd, self.schema)

        hive_table = HiveTimeIndexed(self.database, self.table_name)
        assert_that(hive_table, validly_provides(IHiveTimeIndexed))
        assert_that(hive_table, verifiably_provides(IHiveTimeIndexed))

        hive_table.update(result_frame, 100)
        assert_that(hive_table.rows.collect(), has_length(2))
        assert_that(hive_table, has_property('timestamp', is_(100)))

    def check_historic_table(self, spark):
        historic_list = [(118465,), (118300,)]
        result_rdd = spark.context.parallelize(historic_list)
        result_frame = spark.hive.createDataFrame(result_rdd, self.schema)

        historc_table = HiveTimeIndexedHistoric(self.database,
                                                self.historic_name)
        assert_that(historc_table,
                    validly_provides(IHiveTimeIndexedHistoric))
        assert_that(historc_table,
                    verifiably_provides(IHiveTimeIndexedHistoric))

        data_frame = historc_table.partition(200)
        assert_that(data_frame, is_(none()))

        historc_table.update(result_frame, 200)
        assert_that(historc_table, has_property('timestamps', is_([200])))

        data_frame = historc_table.partition(200)
        assert_that(data_frame, is_not(none()))

        # write into historical using table
        write_to_historical(self.table_name, self.historic_name, 300, spark)
        assert_that(historc_table,
                    has_property('timestamps', is_([300, 200])))

    def check_spark(self, spark):
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

        # 2. create a database
        spark.create_database("orgsync", "home")

        # 3. create initial table
        spark.create_table("categories",
                           columns={"id": "INT",
                                    "name": "STRING"})
        # validate duplicates
        assert_that(calling(spark.create_table).with_args("categories",
                                                          columns={"id": "INT",
                                                                   "name": "STRING"},
                                                          partition_by={"name": "STRING"}),
                    raises(ValueError))

        # 4. Insert data
        # pylint: disable=no-member
        cols = [{'name': 'students', 'id': 1}]
        source = spark.hive.createDataFrame(cols, schema=category_schema)
        # insert into empty table
        spark.insert_into("categories", source, False)
        # test overwrite
        spark.insert_into("categories", source, True)

        # 5. select from table
        df = spark.select_from("categories", ('name',))
        assert_that(df.count(), is_(1))

        # 6. create a simple like table
        spark.create_table("categories_like", like="categories")

        # 7. overwrite table
        overwrite_table("categories", "categories_like", spark)

        # 8. create table with partition
        spark.create_table("assets",
                           columns={"id": "INT",
                                    "name": "STRING"},
                           partition_by={"timestamp": "int"})
        cols = [{'name': 'students', 'id': 1, 'timestamp': 100}]
        source = spark.hive.createDataFrame(cols)
        spark.insert_into("assets", source, False)

        # 9. drop partition
        spark.drop_partition('assets',  {'timestamp': 100})

        # 10. create table with partition
        spark.create_table("historical_categories",
                           like="categories",
                           partition_by={"tstamp": "double"},
                           external=True)
        # 11. describe table
        data = spark.get_table_schema("historical_categories")
        assert_that(data,
                    has_entries('partition', is_(['tstamp']),
                                'tstamp', 'double'))

        # 12. create groups table
        columns = OrderedDict()
        columns['id'] = 'INT'
        columns['name'] = 'STRING'
        columns['accounts'] = 'ARRAY<INT>'
        spark.create_table("groups", columns=columns)

        # insert array
        cols = [{'id': 1, 'name': 'admin', "accounts": [717]}]
        source = spark.hive.createDataFrame(cols, schema=groups_schema)
        spark.insert_into("groups", source, True)

        # 13. coverage select
        assert_that(spark.select_from("unfound", "id", True),
                    is_(none()))

        # 14. drop table
        spark.drop_table('categories_like')
        spark.drop_table('not_found')

    def test_spark(self):
        spark = self.spark()
        self.check_spark(spark)
        self.check_hive_table(spark)
        self.check_historic_table(spark)
