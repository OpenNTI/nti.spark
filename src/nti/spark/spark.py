#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import six

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
            if isinstance(v, six.string_types):
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

    # context manager for testing
    def __enter__(self):
        return self

    def __exit__(self, *unused_args, **unused_kwargs):
        self.close()


@interface.implementer(IHiveSparkInstance)
class HiveSparkInstance(SparkInstance):

    def __init__(self, master, app_name, 
                 location=DEFAULT_LOCATION, log_level=DEFAULT_LOG_LEVEL):
        SparkInstance.__init__(self, master, app_name, log_level)
        self.location = location or DEFAULT_LOCATION
        
    @Lazy
    def conf(self):
        result = super(HiveSparkInstance, self).conf
        # pylint: disable=no-member
        result.set("spark.sql.catalogImplementation", "hive")
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
        create_query = "CREATE TABLE IF NOT EXISTS %s LIKE %s"  % (name, like)
        # pylint: disable=no-member
        self.hive.sql(create_query)

    def create_table(self, name, columns=None, partition_by=None, like=None, external=False):
        """
        Create a hive table
        
        :param name: Table name
        :param columns: (optional) Table columns (vs type) definition
        :param partition_by: (optional) Dictionary of columns vs types to partition a table
        :param like: (optional) Source table name 
        :param external: Create a external table

        :type name: str
        :type columns: dict
        :type partition_by: dict
        :type like: str
        :type external: bool
        """
        external_str = "" if not external else " EXTERNAL "
        create_query = "CREATE %s TABLE IF NOT EXISTS %s" % (external_str, name)
        if like and not partition_by and not external:
            return self.create_table_like(name, like)
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
            if partition_by and set(partition_by.keys()).intersection(set(columns.keys())):
                raise ValueError("Partition column duplicated in columns list")
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
        """
        Return a dataframe with the table data
        
        :param name: Table name
        :param columns: (optional) Iterable of column names

        :type name: str
        :type columns: Iterable
        """
        select_param = []
        for c in columns or ():
            select_param.append("%s" % c)
        select_param = ', '.join(select_param) or '*'
        # pylint: disable=no-member
        return self.hive.sql("SELECT (%s) FROM %s" % (select_param, table))

    def insert_into(self, table, source, overwrite=False):
        """
        Insert into a hive table
        
        :param name: Table name
        :param source: Soruce dataframe
        :param overwrite: Overwrite data flag

        :type name: str
        :type source: dict
        :type overwrite: bool
        """
        # If the source frame is empty, don't do anything
        # because there is nothing to enter
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
