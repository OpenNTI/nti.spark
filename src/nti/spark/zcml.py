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

from zope.interface import Interface

from zope.interface.interface import InterfaceClass

from zope.component.zcml import utility

from zope.configuration import fields

from zope.schema import Choice

from zope.schema.vocabulary import SimpleTerm
from zope.schema.vocabulary import SimpleVocabulary

from nti.spark.interfaces import IHiveTable
from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.spark import HiveSparkInstance

ALL_LEVEL = u'ALL'
INFO_LEVEL = u'INFO'
WARN_LEVEL = u'WARN'
DEBUG_LEVEL = u'DEBUG'
ERROR_LEVEL = u'ERROR'
FATAL_LEVEL = u'FATAL'

LOG_LEVELS = (ALL_LEVEL, INFO_LEVEL, WARN_LEVEL, DEBUG_LEVEL,
              ERROR_LEVEL, FATAL_LEVEL)
LOG_LEVELS_VOCABULARY = \
    SimpleVocabulary([SimpleTerm(_x) for _x in LOG_LEVELS])

logger = __import__('logging').getLogger(__name__)


class IRegisterSparkInstance(Interface):
    """
    Provides a schema for registering a spark Context
    """
    master = fields.TextLine(title=u"Master URL",
                             required=False,
                             default=u"local")

    app_name = fields.TextLine(title=u"Spark App Name",
                               required=False,
                               default=u"Spark App")

    log_level = Choice(vocabulary=LOG_LEVELS_VOCABULARY,
                       title=u'Logging Level',
                       required=False,
                       default=FATAL_LEVEL)


class IRegisterHiveSparkInstance(IRegisterSparkInstance):
    """
    Provides a schema for registering a hive spark instance
    """
    location = fields.TextLine(title=u"Hive data location",
                               required=False,
                               default=None)


def registerHiveSparkInstance(_context, master=u"local", app_name=u"HiveSpark App",
                              location=None, log_level=FATAL_LEVEL):
    factory = functools.partial(HiveSparkInstance,
                                master=master,
                                app_name=app_name,
                                location=location,
                                log_level=log_level)
    utility(_context, provides=IHiveSparkInstance, factory=factory)


class IRegisterHiveTable(Interface):
    """
    Interface representing a registration of a new hive table
    """

    factory = fields.GlobalObject(title=u"The table to register",
                                  required=True)

    provides = fields.GlobalObject(title=u"The interface the factory provides",
                                   required=True)


def registerHiveTable(_context, factory, provides):
    """
    Register a new hive table
    """
    assert (type(provides) is InterfaceClass)

    # Create the Report object to be used as a subscriber
    component = factory()
    assert provides.providedBy(component)

    # register as named hive table utility
    utility(_context, component=component,
            provides=IHiveTable, name=component.table_name)

    # register as nameless utilitiy
    utility(_context, component=component, provides=provides)
