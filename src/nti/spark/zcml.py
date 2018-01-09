#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id:
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=inherit-non-class

import functools

from zope import interface

from zope.component.zcml import utility

from zope.configuration import fields

from nti.spark.interfaces import ISparkInstance
from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.spark import SparkInstance
from nti.spark.spark import HiveSparkInstance

logger = __import__('logging').getLogger(__name__)


class IRegisterSparkInstance(interface.Interface):
    """
    Provides a schema for registering a spark Context
    """
    master = fields.TextLine(title=u"Master URL",
                             required=False,
                             default=u"local")

    app_name = fields.TextLine(title=u"Spark App Name",
                               required=False,
                               default=u"Spark App")


class IRegisterHiveSparkInstance(IRegisterSparkInstance):
    """
    Provides a schema for registering a hive spark Context
    """
    location = fields.TextLine(title=u"Hive data location",
                               required=False,
                               default=u"/user/hive/warehouse")


def registerSparkInstance(_context, master=u"local", app_name=u"Spark App",
                          log_level=u"FATAL"):
    factory = functools.partial(SparkInstance,
                                master=master,
                                appName=app_name,
                                log_level=log_level)

    utility(_context, provides=ISparkInstance, factory=factory)


def registerHiveSparkInstance(_context, master=u"local", app_name=u"Spark App",
                              log_level=u"FATAL"):
    factory = functools.partial(HiveSparkInstance,
                                master=master,
                                appName=app_name,
                                log_level=log_level)

    utility(_context, provides=IHiveSparkInstance, factory=factory)
