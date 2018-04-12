#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=inherit-non-class

from zope import interface

from nti.schema.field import Int
from nti.schema.field import Object
from nti.schema.field import IndexedIterable


class IRDD(interface.Interface):
    """
    Interface for matching parallelized
    collections
    """


class IDataFrame(interface.Interface):
    """
    Interface representing a pyspark
    data frame
    """


class ISparkContext(interface.Interface):
    """
    The Spark Context necessary for computation
    and persistence of RDDs
    """


class ISparkSession(interface.Interface):
    """
    A Spark Session linked to an instance
    of a Spark Context for converting
    RDDs to DataFrames
    """


class IHiveContext(interface.Interface):
    """
    A Spark Session linked to an instance
    of a Spark Context for converting
    RDDs to DataFrames
    """


class ISparkInstance(interface.Interface):
    """
    An object encapsulating both a Spark Context
    and its attached Spark Session.
    """

    spark = Object(ISparkContext,
                   title=u"Spark Context")

    session = Object(ISparkSession,
                     title=u"Spark Session")

    def close():
        """
        Close both the session and context for this instance.
        """


class IHiveSparkInstance(ISparkInstance):
    """
    An object encapsulating both a Hive Context
    """
    hive = Object(IHiveContext,
                  title=u"Hive Context")

    def database_exists(name):
        """
        Checks if a database with the specified name exists

        :param name: Database name

        :type name: str
        """

    def create_database(name, location=None):
        """
        Create a Hive Database

        :param name: Database name
        :param location: (Optional) HDFS path

        :type name: str
        :type location: str
        """

    def table_exists(name):
        """
        Checks if a table with the specified name exists

        :param name: Table name

        :type name: str
        """

    def get_table_schema(table):
        """
        Get a table's schema as a dictionary
        of fields and their data types, along
        with an entry noting the partitioned values
        """

    def is_partitioned(table):
        """
        Return if a table has any partitioned attributes.
        If there is, return the list of those attributes
        """

    def drop_table(name):
        """
        Drop the specified table is exists
        
        :param name: Table name

        :type name: str
        """
     
    def create_table(name, partition_by=None, columns=None, like=None,
                     external=False, storage='ORC'):
        """
        Create a hive table

        :param name: Table name
        :param columns: (optional) Table columns (vs type) definition
        :param partition_by: (optional) Dictionary of columns vs types to partition a table
        :param like: (optional) Source table name 
        :param external: Create a external table
        :param storage: Storage method (e.g. ORC, PARQUET)

        :type name: str
        :type columns: dict
        :type partition_by: dict
        :type like: str
        :type external: bool
        :type storage: str
        """

    def drop_partition(table, partition):
        """
        Drop a partition from a hive table

        :param table: Table name
        :param columns: Map of partition column names vs values

        :type name: str
        :type columns: dict
        """

    def select_from(table, columns=None):
        """
        Return a dataframe with the table data

        :param name: Table name
        :param columns: (optional) Iterable of column names

        :type name: str
        :type columns: Iterable
        """

    def insert_into(table, source, overwrite=False):
        """
        Insert into a hive table

        :param name: Table name
        :param source: Soruce dataframe
        :param overwrite: Overwrite data flag

        :type name: str
        :type source: dict
        :type overwrite: bool
        """


class IHiveTable(interface.Interface):
    """
    Interface marking a table either managed or
    external in Hive
    """

    table_name = interface.Attribute("Name of the table to update")

    database = interface.Attribute("Database name")

    rows = Object(IDataFrame,
                  title=u"Row contents of hive table",
                  required=False,
                  default=None)

    def update(new_frame, overwrite=True):
        """
        Archive the data in the frame

        :param new_frame: The class:`nti.spark.interfaces.IDataFrame` to archive
        :param overwrite: Overwrite table flag
        """


class IHiveTimeIndexed(IHiveTable):
    """
    Interface marking a class:`IHiveTable` as
    begin indexed by a timestamp
    """

    timestamp = Int(title=u"Timestamp of current load",
                    required=False,
                    default=None)

    def update(new_frame, timestamp=None, overwrite=True):  # pylint: disable=arguments-differ
        """
        Archive the data in the frame

        :param new_frame:  The class:`nti.spark.interfaces.IDataFrame` to archive
        :param timestamp: The timestamp
        :param overwrite: Overwrite table flag
        """


class IHiveTimeIndexedHistorical(IHiveTable):
    """
    Interface marking a class:`IHiveTable` as being
    indexed by a timestamp and keeping historic logs
    of previous events
    """

    timestamps = IndexedIterable(title=u"The past timestamps",
                                 min_length=0,
                                 required=False,
                                 default=None,
                                 value_type=Int(title=u"The id"))

    def partition(timestamp):
        """
        Return the data associated for the specified timestamp
        
        :param timestamp: The timestamp
        """
    
    def update(new_frame, timestamp=None, overwrite=True):  # pylint: disable=arguments-differ
        """
        Archive the data in the frame

        :param new_frame:  The class:`nti.spark.interfaces.IDataFrame` to archive
        :param timestamp: The timestamp
        :param overwrite: Overwrite table flag
        """
IHiveTimeIndexedHistoric = IHiveTimeIndexedHistorical  # BWC


class IArchivableHiveTimeIndexed(IHiveTable):

    timestamp = Int(title=u"Timestamp of current load",
                    required=False,
                    default=None)
    
    def historical():
        """
        Return the class:`.IArchivableHiveTimeIndexedHistorical` that holds historical data
        """
    
    def reset():
        """
        Reset/Drop this table
        """

    def archive():
        """
        Archive this table
        """

    def update(new_frame, timestamp=None, archive=True, reset=False, ovewrrite=True):  # pylint: disable=arguments-differ
        """
        Archive the data in the frame

        :param new_frame:  The class:`nti.spark.interfaces.IDataFrame` to archive
        :param timestamp: The timestamp
        :param archive: Archive stored data
        :param reset: Drop table
        :param overwrite: Overwrite table flag
        """


class IArchivableHiveTimeIndexedHistorical(IHiveTimeIndexedHistoric):

    timestamps = IndexedIterable(title=u"The past timestamps",
                                 min_length=0,
                                 required=False,
                                 default=None,
                                 value_type=Int(title=u"The id"))

    def current():
        """
        Return the class:`.IArchivableHiveTimeIndexed` that holds current values
        """

    def unarchive(timestamp, archive=True, ovewrrite=True):
        """
        Unarchive the values from the partition specified by the time stamp
        
        :param timestamp: The timestamp
        :param archive: Archive stored data
        :param overwrite: Overwrite table flag
        """
