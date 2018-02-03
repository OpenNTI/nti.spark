#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import time

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

#: pyspark.sql.functions.lit
LIT_FUNC = getattr(functions, 'lit')

logger = __import__('logging').getLogger(__name__)


def get_timestamp(timestamp=None):
    timestamp = time.time() if timestamp is None else timestamp
    return int(timestamp)


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
    timestamp = get_timestamp(timestamp)
    spark = component.getUtility(IHiveSparkInstance) if not spark else spark
    # copy into historical table
    table = spark.hive.table(target)
    columns = list(table.columns)
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
    spark = component.getUtility(IHiveSparkInstance) if not spark else spark
    table = spark.hive.table(target)
    columns = ','.join(table.columns)
    overwrite = 'OVERWRITE' if overwrite else ''
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

    def __init__(self, database, table_name, overwrite=True):
        self.database = database
        self.overwrite = overwrite
        self.table_name = table_name

    def create_table_like(self, like, spark=None):
        spark = component.getUtility(IHiveSparkInstance) if not spark else spark
        return spark.create_table(self.table_name, like=like, external=True)

    def write_to_hive(self, new_frame, spark=None):
        # create temp frame
        new_frame.createOrReplaceTempView("new_frame")
        # create database
        spark = component.getUtility(IHiveSparkInstance) if not spark else spark
        spark.create_database(self.database)
        # create table
        self.create_table_like("new_frame", spark)
        # insert new data
        spark.insert_into(self.table_name, new_frame,
                          overwrite=self.overwrite)
        spark.hive.dropTempTable('new_frame')

    def update(self, new_frame):
        assert IDataFrame.providedBy(new_frame), "Invalid DataFrame"
        self.write_to_hive(new_frame)

    @property
    def rows(self):
        hive = component.getUtility(IHiveSparkInstance)
        return hive.select_from(self.table_name)


class HiveTimeMixin(object):

    def get_timestamp(self, timestamp=None):
        return get_timestamp(timestamp)

    def update(self, new_frame, timestamp=None):  # pylint: disable=arguments-differ
        assert IDataFrame.providedBy(new_frame), "Invalid DataFrame"
        # add timestamp to frame
        timestamp = self.get_timestamp(timestamp)
        frame = new_frame.withColumn(TIMESTAMP, LIT_FUNC(timestamp))
        # write frame
        return super(HiveTimeMixin, self).update(frame)


@interface.implementer(IHiveTimeIndexed)
class HiveTimeIndexed(HiveTimeMixin, HiveTable):

    @property
    def timestamp(self):
        hive = component.getUtility(IHiveSparkInstance)
        query_result = hive.select_from(self.table_name,
                                        columns=(TIMESTAMP,),
                                        distinct=True)
        if query_result is not None:
            data = query_result.collect()
            return getattr(data.pop(), TIMESTAMP) if data else None


@interface.implementer(IHiveTimeIndexedHistoric)
class HiveTimeIndexedHistoric(HiveTimeMixin, HiveTable):

    def create_table_like(self, like, spark=None):
        spark = component.getUtility(IHiveSparkInstance) if not spark else spark
        return spark.create_table(self.table_name,
                                  like=like,
                                  external=True,
                                  partition_by={TIMESTAMP: TIMESTAMP_TYPE})

    @property
    def timestamps(self):
        hive = component.getUtility(IHiveSparkInstance)
        query_result = hive.select_from(self.table_name,
                                        columns=(TIMESTAMP,),
                                        distinct=True)
        if query_result is not None:
            # Return newest first
            return sorted((getattr(r, TIMESTAMP) for r in query_result.collect() or ()),
                          reverse=True)
