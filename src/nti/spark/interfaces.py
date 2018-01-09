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


class ISparkInstance(interface.Interface):
    """
    An object encapsulating both a Spark Context
    and its attached Spark Session.
    """
    context = Object(ISparkContext,
                     title=u"Spark Context")

    session = Object(ISparkSession,
                     title=u"Spark Session")

    def close():
        """
        Close both the session and context for this instance.
        """
