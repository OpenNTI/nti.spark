#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import six
import collections

from pyspark import SparkContext

from pyspark.conf import SparkConf

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
from nti.spark import DEFAULT_STORAGE_FORMAT

from nti.spark.interfaces import ISparkInstance
from nti.spark.interfaces import IHiveSparkInstance

logger = __import__('logging').getLogger(__name__)


def _columns_as_str(column_dict):
    """
    Convert dictionary of name, type
    pairs to hive-compatible string
    """
    result = []
    for key, value in column_dict.items():
        result.append("%s %s" % (key, value))
    return ', '.join(result)


def _quote(v):
    if isinstance(v, six.string_types):
        return "'%s'" % v.replace("'", r"\'")
    return "%s" % v


def _dataframe_as_str(data_frame):
    """
    Convert a data frame to a
    hive-compatible string
    """
    result = []
    for row in data_frame.toLocalIterator():
        items = []
        for v in row:
            if isinstance(v, (list, tuple, set)):
                values = (_quote(x) for x in v)
                items.append("array(%s)" % ','.join(values))
            else:
                items.append(_quote(v))
        result.append("(%s)" % ','.join(items))
    result = ','.join(result)
    return result


def _match_schema(table, source):
    """
    Re-arrange the source data frame
    to match schema ordering of the
    internal table
    """
    # Both column lists must contain the same data
    assert set(source.columns) == set(table.columns)
    return source.select(*table.columns)


@interface.implementer(ISparkInstance)
class SparkInstance(SchemaConfigured):
    createDirectFieldProperties(ISparkInstance)

    # pylint: disable=super-init-not-called
    def __init__(self, master, app_name, log_level=DEFAULT_LOG_LEVEL):
        self.master = master
        self.app_name = app_name
        self.log_level = DEFAULT_LOG_LEVEL if not log_level else log_level

    @Lazy
    def conf(self):
        conf = SparkConf()
        conf.setMaster(self.master)
        conf.setAppName(self.app_name)
        return conf

    @Lazy
    def spark(self):
        result = SparkContext.getOrCreate(self.conf)
        result.setLogLevel(self.log_level)  # pylint: disable=no-member
        return result
    context = spark

    @Lazy
    def session(self):
        return SparkSession(self.spark)

    def close(self):
        # pylint: disable=no-member
        if 'session' in self.__dict__:
            self.session.stop()
        if 'spark' in self.__dict__:
            self.spark.stop()


@interface.implementer(IHiveSparkInstance)
class HiveSparkInstance(SparkInstance):

    def __init__(self, master, app_name,
                 location=DEFAULT_LOCATION, log_level=DEFAULT_LOG_LEVEL):
        SparkInstance.__init__(self, master, app_name, log_level)
        self.location = DEFAULT_LOCATION if location is None else location

    @Lazy
    def conf(self):
        result = super(HiveSparkInstance, self).conf
        # pylint: disable=no-member
        result.set("spark.sql.catalogImplementation", "hive")
        # result.set("spark.scheduler.mode", "FAIR")
        return result

    @Lazy
    def hive(self):
        return HiveContext(self.spark)

    def get_table_schema(self, table):
        # pylint: disable=no-member
        schema = {PARTITION_KEY: []}
        df = self.hive.sql("DESCRIBE %s" % (table))
        coll = df.select(df.col_name, df.data_type).collect()
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

    def create_database(self, name, location=None):
        create_query = "CREATE DATABASE IF NOT EXISTS %s" % name
        create_query += " LOCATION '%s'" % location if location else ""
        # pylint: disable=no-member
        self.hive.sql(create_query)

    def drop_table(self, name):
        drop_query = "DROP TABLE IF EXISTS %s" % name
        # pylint: disable=no-member
        return self.hive.sql(drop_query)

    def create_table_like(self, name, like):
        """
        Create a simple hive table like

        :param name: Table name
        :param like: Source table name

        :type name: str
        :type like: str
        """
        # Use LIKE keyword if we are
        # not trying to do anything additional
        create_query = "CREATE TABLE IF NOT EXISTS %s LIKE %s" % (name, like)
        # pylint: disable=no-member
        return self.hive.sql(create_query)

    def create_table(self, name, columns=None, partition_by=None, like=None,
                     external=False, storage=DEFAULT_STORAGE_FORMAT):
        if not external:
            create_query = "CREATE TABLE IF NOT EXISTS %s" % name
        else:
            create_query = "CREATE EXTERNAL TABLE IF NOT EXISTS %s" % name
        # analyze params
        # simple like table
        if like and not partition_by and not external:
            return self.create_table_like(name, like)
        # like table w/ partition or external
        elif like and (partition_by or external):
            like_schema = self.get_table_schema(like)
            # Get all copied partition values
            partition_cols = {
                x: like_schema[x] for x in like_schema[PARTITION_KEY]
            }
            # Add in any additional marked partition columns
            if partition_by:
                for key, value in partition_by.items():
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
        # creating a regular table
        else:
            # check columns
            assert columns, "Must specify columns"
            # valdiate partition columns
            if partition_by and set(partition_by.keys()).intersection(set(columns.keys())):
                raise ValueError("Found duplicate column(s) in table definition")
            # add column to query
            create_query += " (%s)" % _columns_as_str(columns)
            if partition_by:
                create_query += " PARTITIONED BY (%s)" % _columns_as_str(partition_by)
        # always store as parquet file
        create_query += " STORED AS %s" % storage
        if external:
            location = name if not self.location else "%s/%s" % (self.location, name)
            create_query += " LOCATION '%s'" % location
        # pylint: disable=unused-variable
        __traceback_info__ = create_query
        # pylint: disable=no-member
        result = self.hive.sql(create_query)
        return result

    def drop_partition(self, table, partition):
        assert isinstance(partition, collections.Mapping)
        if self.is_partitioned(table) and partition:
            query = ','.join('%s=%s' % (name, value)
                             for name, value in partition.items())
            query = 'ALTER TABLE %s DROP IF EXISTS PARTITION(%s)' % (table, query)
            # pylint: disable=unused-variable
            __traceback_info__ = query
            # pylint: disable=no-member
            result = self.hive.sql(query)
            return result

    def select_from(self, table, columns=None, distinct=False):
        select_param = ["%s" % c for c in columns or ()]
        select_param = ','.join(select_param) or '*'
        distinct_param = "" if not distinct else "DISTINCT"
        __traceback_info__ = "SELECT %s (%s) FROM %s" % (distinct_param, select_param, table)
        try:
            # pylint: disable=no-member
            result = self.hive.sql(__traceback_info__)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error while executing select statement '%s'",
                             __traceback_info__)
            result = None
        return result

    def insert_into(self, table, source, overwrite=False):
        # If the source frame is empty, don't do anything
        # because there is nothing to enter
        # pylint: disable=no-member
        source = _match_schema(self.hive.table(table), source)
        if source.count():
            partition_str = ""
            # Get any partitioned columns
            partition_col = self.is_partitioned(table)
            if partition_col:
                partition_str += "("
                # Select the partition column
                partition_val = source.select(partition_col).collect()[0]
                # Convert to partition insert string
                for partition in partition_col:
                    value = getattr(partition_val, partition)
                    partition_str += "%s=%s," % (partition, value)
                partition_str = partition_str[:-1] + ")"
            # Inidicate overwrite
            if not overwrite:
                insert_param = "INSERT INTO %s " % (table,)
            else:
                insert_param = "INSERT OVERWRITE TABLE %s " % (table,)
            # Add in partition if necessary
            if partition_str:
                insert_param += "PARTITION %s " % (partition_str)
                source = source.drop(*partition_col)
            # Convert the source dataframe into
            # a query string
            values = _dataframe_as_str(source)
            insert_param += "VALUES %s" % (values)
            # pylint: disable=no-member
            self.hive.sql(insert_param)
