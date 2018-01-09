#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyspark import SparkContext

from pyspark.sql import HiveContext
from pyspark.sql import SparkSession

from zope import interface

from zope.cachedescriptors.property import Lazy

from nti.schema.fieldproperty import createDirectFieldProperties

from nti.schema.schema import SchemaConfigured

from nti.spark import PARTITION_KEY
from nti.spark import DEFAULT_LOCATION
from nti.spark import DEFAULT_LOG_LEVEL
from nti.spark import PARITION_INFORMATION

from nti.spark.interfaces import ISparkInstance
from nti.spark.interfaces import IHiveSparkInstance

logger = __import__('logging').getLogger(__name__)


def _columns_as_str(column_dict):
    """
    Convert dictionary of name, type
    pairs to hive-compatible string
    """
    result_str = ""
    for key, value in column_dict.iteritems():
        result_str += "%s %s," % (key, value)
    return result_str[:-1]


def _dataframe_as_str(data_frame):
    """
    Convert a data frame to a
    hive-compatible string
    """
    rows = data_frame.collect()
    result = ""
    for r in rows:
        item = "("
        for v in r:
            if type(v) == unicode:
                # Quote string types
                item += "'%s'," % (v)
            else:
                item += "%s," % (v)
        item = item[:-1] + ")"
        result += item + ","
    return result[:-1]


@interface.implementer(ISparkInstance)
class SparkInstance(SchemaConfigured):
    createDirectFieldProperties(ISparkInstance)

    # pylint: disable=super-init-not-called
    def __init__(self, master=None, appName=None, log_level=DEFAULT_LOG_LEVEL):
        self._spark = SparkContext(master=master, appName=appName)
        self._spark.setLogLevel(DEFAULT_LOG_LEVEL if not log_level else log_level)

    @property
    def spark(self):
        return self._spark
    context = spark

    @Lazy
    def session(self):
        return SparkSession(self.spark)

    def close(self):
        if 'session' in self.__dict__:
            # pylint: disable=no-member
            self.session.stop()
        self._spark.stop()


@interface.implementer(IHiveSparkInstance)
class HiveSparkInstance(SparkInstance):

    def __init__(self, master=None, appName=None, 
                 location=DEFAULT_LOCATION, log_level=DEFAULT_LOG_LEVEL):
        SparkInstance.__init__(self, master, appName, log_level)
        self.location = location or DEFAULT_LOCATION
        
    @Lazy
    def hive(self):
        return HiveContext(self.spark)

    def get_table_schema(self, table):
        # pylint: disable=no-member
        df = self.hive.sql("DESCRIBE %s" % (table))
        coll = df.select(df.col_name, df.data_type).collect()
        schema = {PARTITION_KEY: []}
        has_seen_partition = False
        for row in coll:
            # Iterating in order, note when we've seen
            # the partition comment
            if row.col_name == PARITION_INFORMATION:
                has_seen_partition = True
            # Add if not a comment
            if not row.col_name.startswith('#'):
                schema[row.col_name] = row.data_type
                # If is partition, add to list
                if has_seen_partition:
                    schema[PARTITION_KEY].append(row.col_name)
        return schema

    def is_partitioned(self, table):
        schema = self.get_table_schema(table)
        return schema[PARTITION_KEY] if schema[PARTITION_KEY] else False

    def create_table(self, name, partition_by=None, columns=None, like=None, external=False):
        external_str = "" if not external else " EXTERNAL "
        create_query = "CREATE %s TABLE IF NOT EXISTS %s" % (external_str, name)
        if like and not partition_by and not external:
            # Use LIKE keyword if we are
            # not trying to do anything additional
            create_query += " LIKE %s" % (like)
            # pylint: disable=no-member
            self.hive.sql(create_query)
            return
        elif like and (partition_by or external):
            like_schema = self.get_table_schema(like)
            # Get all copied partition values
            partition_cols = {
                x: like_schema[x] for x in like_schema[PARTITION_KEY]
            }
            # Add in any additional marked partition columns
            if partition_by:
                for key, value in partition_by.iteritems():
                    partition_cols[key] = value
            # Convert column collections to query strings
            partition_cols_str = _columns_as_str(partition_cols)
            reg_cols_str = _columns_as_str({
                    x: like_schema[x] for x in like_schema.keys() 
                    if x not in partition_cols.keys() and x != PARTITION_KEY
                })
            create_query += " (%s)" % (reg_cols_str)
            if partition_by or partition_cols:
                create_query += " PARTITIONED BY (%s)" % (partition_cols_str)
        else:
            if partition_by in columns.keys():
                raise ValueError("Partition column duplicate in columns list")
            column_str = _columns_as_str(columns)
            create_query += " (%s)" % (column_str)
            if partition_by:
                create_query += " PARTITIONED BY %s" % _columns_as_str(partition_by)
        # Always store as parquet file
        create_query += " STORED AS PARQUET"
        if external:
            create_query += " LOCATION '%s/%s/'" % (self.location, name)
        # pylint: disable=no-member
        self.hive.sql(create_query)

    def select_from(self, table, columns=None):
        if columns is None:
            # If all, use *
            select_param = "*"
        else:
            select_param = ""
            for c in columns:
                select_param += "%s," % (c)
            select_param = select_param[:-1]
        # pylint: disable=no-member
        return self.hive.sql("SELECT (%s) FROM %s" % (select_param, table))

    def insert_into(self, table, source, overwrite=False):
        # If the source frame is empty, don't do anything
        # because there is nothing to enter
        if source.count() < 1:
            return
        # Get any partitioned columns
        partition_col = self.is_partitioned(table)
        partition_str = ""
        if partition_col:
            # Select the partition column
            partition_val = source.select(partition_col).collect()[0]
            partition_str += "("
            # Convert to partition insert string
            for partition in partition_col:
                partition_str += "%s=%s," % (partition,
                                             getattr(partition_val, partition))
            partition_str = partition_str[:-1] + ")"
        # Inidicate overwrite
        if not overwrite:
            insert_param = "INSERT INTO %s " % (table)
        else:
            insert_param = "INSERT OVERWRITE TABLE %s " % (table)
        # Add in partition if necessary
        if partition_str:
            insert_param += "PARTITION %s " % (partition_str)
            source = source.drop(*partition_col)
        # Convert the source dataframe into
        # a query string
        source_str = _dataframe_as_str(source)
        insert_param += "VALUES %s" % (source_str)
        # pylint: disable=no-member
        self.hive.sql(insert_param)
