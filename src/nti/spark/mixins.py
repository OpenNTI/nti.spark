#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from nti.spark import TIMESTAMP

from nti.spark.interfaces import IDataFrame
from nti.spark.interfaces import IHiveSparkInstance
from nti.spark.interfaces import IArchivableHiveTimeIndexed
from nti.spark.interfaces import IArchivableHiveTimeIndexedHistorical

from nti.spark.hive import LIT_FUNC

from nti.spark.hive import HiveTimeIndexed
from nti.spark.hive import HiveTimeIndexedHistoric

from nti.spark.hive import insert_into_table
from nti.spark.hive import write_to_historical

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IArchivableHiveTimeIndexed)
class ABSArchivableHiveTimeIndexed(HiveTimeIndexed):

    empty_frame = False

    def historical(self):
        raise NotImplementedError()

    def reset(self, spark=None):
        spark = component.getUtility(IHiveSparkInstance) if not spark else spark
        logger.warning("Dropping table %s", self.table_name)
        spark.drop_table(self.table_name)

    def archive(self):
        rows = self.rows
        historical = self.historical()
        if rows is not None and historical is not None:
            historical.drop_partition(self.timestamp)
            historical.update(rows, self.timestamp)
    _archive = archive  # BWC

    def write_to_hive(self, new_data, overwrite=True, spark=None):  # pylint: disable=arguments-differ
        # create database
        spark = component.getUtility(IHiveSparkInstance) if not spark else spark
        if not spark.database_exists(self.database):
            spark.create_database(self.database)
        #  create table
        new_data.createOrReplaceTempView("new_data")
        if not spark.table_exists(self.table_name):
            self.create_table_like("new_data", spark)
        # insert data
        insert_into_table("new_data", self.table_name, overwrite, spark)
        # clean up
        spark.hive.dropTempTable('new_data')

    def update(self, new_data, timestamp=None, archive=True, reset=False, overwrite=True):  # pylint: disable=arguments-differ
        if archive:
            self.archive()
        if reset:
            self.reset()
        super(ABSArchivableHiveTimeIndexed, self).update(new_data, timestamp, overwrite)


@interface.implementer(IArchivableHiveTimeIndexedHistorical)
class ABSArchivableHiveTimeIndexedHistorical(HiveTimeIndexedHistoric):

    empty_frame = False

    def current(self):
        raise NotImplementedError()

    def unarchive(self, timestamp, archive=True, overwrite=True, spark=None):
        current = self.current()
        if current is not None:
            if archive:
                current.archive() 
            data_frame = self.partition(timestamp, spark)
            if data_frame is not None:
                current.update(data_frame, timestamp, overwrite)
            return data_frame

    def update(self, data_frame, timestamp=None, spark=None):  # pylint: disable=arguments-differ
        assert IDataFrame.providedBy(data_frame), "Invalid DataFrame"
        # create database
        spark = component.getUtility(IHiveSparkInstance) if not spark else None
        if not spark.database_exists(self.database):  # pragma: no cover
            spark.create_database(self.database)
        # prepare dataframe
        timestamp = self.get_timestamp(timestamp)
        data_frame.createOrReplaceTempView("archive_data")
        data_frame = data_frame.withColumn(TIMESTAMP, LIT_FUNC(timestamp))
        # create table and insert
        if not spark.table_exists(self.table_name):
            self.create_table_like("archive_data", spark)
        write_to_historical("archive_data", self.table_name, timestamp, spark)
        # clean up
        spark.hive.dropTempTable('archive_data')
