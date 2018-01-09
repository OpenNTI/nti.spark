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

from nti.schema.field import Object


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

    def create_table(name, partition_by=None, columns=None, like=None, external=False):
        """
        Create a Hive Table
        """

    def select_from(table, columns=None):
        """
        Select values from a table
        """

    def insert_into(table, source, overwrite=False):
        """
        Insert values from a source into the table.
        Indicate overwrite existing tables if necessary
        """
