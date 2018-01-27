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


@interface.implementer(IHiveTable)
class HiveTable(object):

    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name

    def create_table(self, hive, like):
        return hive.create_table(self.table_name, like=like, external=True)

    def _write_to_hive(self, new_frame):
        # create temp frame
        new_frame.createOrReplaceTempView("new_frame")
        # create database
        hive = component.getUtility(IHiveSparkInstance)
        hive.create_database(self.database)
        # create table
        self.create_table(hive, "new_frame")
        # insert new data
        hive.insert_into(self.table_name, new_frame, overwrite=True)
        hive.hive.dropTempTable('new_frame')

    @classmethod
    def get_timestamp(cls, timestamp=None):
        timestamp = time.time() if timestamp is None else timestamp
        return int(timestamp)

    def update(self, new_frame, timestamp=None):
        if not IDataFrame.providedBy(new_frame):
            raise TypeError("Cannot update non-DataFrame")
        timestamp = self.get_timestamp(timestamp)
        frame = new_frame.withColumn(TIMESTAMP, LIT_FUNC(timestamp))
        self._write_to_hive(frame)

    @property
    def rows(self):
        hive = component.getUtility(IHiveSparkInstance)
        return hive.select_from(self.table_name)


@interface.implementer(IHiveTimeIndexed)
class HiveTimeIndexed(HiveTable):

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
class HiveTimeIndexedHistoric(HiveTable):

    def create_table(self, hive, like):
        return hive.create_table(self.table_name, 
                                 like=like,
                                 external=True,
                                 partition_by={TIMESTAMP: TIMESTAMP_TYPE})

    # helper write methods

    @classmethod
    def write_to_historical(cls, source, target, timestamp=None):
        """
        Insert the data from the source table to a target partitioned table
    
        :param source: Source table name
        :param target: Target (partitioned) table name
        :param timestamp: (optional) Timestamp
    
        :type source: str
        :type target: str
        :type timestamp: int
        """
        timestamp = cls.get_timestamp(timestamp)
        spark = component.getUtility(IHiveSparkInstance)
        # copy into historical table
        table = spark.hive.table(target)
        columns = list(table.columns)
        columns.remove(TIMESTAMP)
        query = ["INSERT INTO TABLE %s" % target,
                 "PARTITION (%s=%s) " % (TIMESTAMP, timestamp),
                 "SELECT %s FROM %s" % (','.join(columns), source)]
        return spark.hive.sql(' '.join(query))
    
    def write_from(self, source, timestamp=None):
        return self.write_to_historical(source, self.table_name, timestamp)

    # inteface properties

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