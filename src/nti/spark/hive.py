#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyspark.sql import functions

from zope import component
from zope import interface

from nti.spark import TIMESTAMP
from nti.spark import TIMESTAMP_TYPE

from nti.spark.interfaces import IDataFrame
from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IHiveTimeIndexed
from nti.spark.interfaces import IHiveSparkInstance
from nti.spark.interfaces import IHiveTimeIndexedHistoric

from nti.spark.utils import get_timestamp

#: pyspark.sql.functions.lit
LIT_FUNC = getattr(functions, 'lit')

logger = __import__('logging').getLogger(__name__)


def get_spark(spark=None):
    return component.getUtility(IHiveSparkInstance) if not spark else spark


def write_to_historical(source, target, timestamp=None, spark=None):
    """
    Insert the data from the source table to a target partitioned table

    :param source: Source table name
    :param target: Target (partitioned) table name
    :param timestamp: (optional) Timestamp

    :type source: str
    :type target: str
    :type timestamp: int
    """
    spark = get_spark(spark)
    timestamp = get_timestamp(timestamp)
    # copy into historical table
    table = spark.hive.table(target)
    columns = list(table.columns)
    if TIMESTAMP in columns:
        columns.remove(TIMESTAMP)
    # execute query
    query = ["INSERT INTO TABLE %s" % target,
             "PARTITION (%s=%s) " % (TIMESTAMP, timestamp),
             "SELECT %s FROM %s" % (','.join(columns), source)]
    return spark.hive.sql(' '.join(query))


def insert_into_table(source, target, overwrite=False, spark=None):
    """
    Insert into the target table using the data from the source

    :param source: Source table name
    :param target: Target (partitioned) table name
    :param overwrite: Overwrite flag

    :type source: str
    :type target: str
    :type overwrite: bool
    """
    spark = get_spark(spark)
    table = spark.hive.table(target)
    columns = ','.join(table.columns)
    overwrite = 'OVERWRITE' if overwrite else 'INTO'
    query = """
            INSERT %s TABLE %s
            SELECT %s FROM %s
            """ % (overwrite, target, columns, source)
    query = ' '.join(query.split())
    return spark.hive.sql(query)


def overwrite_table(source, target, spark=None):
    """
    overwrite the target table using the data from the source
    """
    return insert_into_table(source, target, True, spark)


@interface.implementer(IHiveTable)
class HiveTable(object):

    empty_frame = True

    def __init__(self, database, table_name, external=False):
        self.database = database
        self.external = external
        self.table_name = table_name

    @property
    def __name__(self):
        return self.table_name

    def create_table_like(self, like, spark=None):
        spark = get_spark(spark)
        return spark.create_table(self.table_name, like=like, external=self.external)

    def write_to_hive(self, new_frame, overwrite=True, spark=None):
        # create temp frame
        new_frame.createOrReplaceTempView("new_frame")
        # create database
        spark = get_spark(spark)
        spark.create_database(self.database)
        # create table
        self.create_table_like("new_frame", spark)
        # insert new data
        spark.insert_into(self.table_name, new_frame,
                          overwrite=overwrite)
        spark.hive.catalog.dropTempView('new_frame')

    def update(self, new_frame, overwrite=True):
        assert IDataFrame.providedBy(new_frame), "Invalid DataFrame"
        self.write_to_hive(new_frame, overwrite)

    @property
    def rows(self):
        hive = get_spark()
        return hive.select_from(self.table_name, None, False, self.empty_frame)


class HiveTimeMixin(object):

    def get_timestamp(self, timestamp=None):
        return get_timestamp(timestamp)

    def update(self, new_frame, timestamp=None, overwrite=True):  # pylint: disable=arguments-differ
        assert IDataFrame.providedBy(new_frame), "Invalid DataFrame"
        # add timestamp to frame
        timestamp = self.get_timestamp(timestamp)
        frame = new_frame.withColumn(TIMESTAMP, LIT_FUNC(timestamp))
        # write frame
        return super(HiveTimeMixin, self).update(frame, overwrite)


@interface.implementer(IHiveTimeIndexed)
class HiveTimeIndexed(HiveTimeMixin, HiveTable):

    @property
    def timestamp(self):
        hive = get_spark()
        query_result = hive.select_from(self.table_name,
                                        columns=(TIMESTAMP,),
                                        distinct=True)
        if query_result is not None:
            data = query_result.collect()
            return getattr(data.pop(), TIMESTAMP) if data else None


@interface.implementer(IHiveTimeIndexedHistoric)
class HiveTimeIndexedHistoric(HiveTimeMixin, HiveTable):

    def create_table_like(self, like, spark=None):
        spark = get_spark(spark)
        return spark.create_table(self.table_name,
                                  like=like,
                                  external=self.external,
                                  partition_by={TIMESTAMP: TIMESTAMP_TYPE})

    def partition(self, timestamp, spark=None):
        spark = get_spark(spark)
        timestamp = self.get_timestamp(timestamp)
        query = "SELECT * FROM %s WHERE %s=%s" % (self.table_name, TIMESTAMP, timestamp)
        __traceback_info__ = query
        try:
            # pylint: disable=no-member
            result = spark.hive.sql(__traceback_info__)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error while executing select statement '%s'",
                             __traceback_info__)
            result = None
        return result

    def drop_partition(self, timestamp, spark=None):
        spark = get_spark(spark)
        timestamp = self.get_timestamp(timestamp)
        partition = {TIMESTAMP: timestamp}
        if spark.table_exists(self.table_name):
            spark.drop_partition(self.table_name, partition)

    @property
    def timestamps(self):
        hive = get_spark()
        query_result = hive.select_from(self.table_name,
                                        columns=(TIMESTAMP,),
                                        distinct=True)
        if query_result is not None:
            # Return newest first
            return sorted((getattr(r, TIMESTAMP) for r in query_result.collect() or ()),
                          reverse=True)
