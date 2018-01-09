#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyspark import SparkContext

from pyspark.sql import SparkSession

from zope import interface

from nti.schema.fieldproperty import createDirectFieldProperties

from nti.schema.schema import SchemaConfigured

from nti.spark.interfaces import ISparkInstance


@interface.implementer(ISparkInstance)
class SparkInstance(SchemaConfigured):
    createDirectFieldProperties(ISparkInstance)

    # pylint: disable=super-init-not-called
    def __init__(self, master=None, appName=None):
        self._context = SparkContext(master=master, appName=appName)
        self._session = SparkSession(self._context)

    @property
    def context(self):
        return self._context

    @property
    def session(self):
        return self._session

    def close(self):
        # Function only meant to be
        # used for testing purposes as
        # to avoid multiple contexts.
        self._session.stop()
        self._context.stop()
