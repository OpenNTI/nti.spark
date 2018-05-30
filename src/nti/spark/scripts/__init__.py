#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import time
import logging

import zope.exceptions.log

from zope.configuration import config
from zope.configuration import xmlconfig

from zope.dottedname import resolve as dottedname

from nti.spark.utils import parse_date
from nti.spark.utils import get_timestamp as make_timestamp

DATASERVER_ETC_DIR = 'DATASERVER_ETC_DIR'
DEFAULT_LOG_FORMAT = '[%(asctime)-15s] [%(name)s] %(levelname)s: %(message)s'

TAB_SEPARATORS = {
    False: ',',
    True: '\t'
}

TAB_ENCODING = {
    False: 'UTF-8',
    True: 'UTF-16',
}

logger = __import__('logging').getLogger(__name__)


def configure_logging(fmt=DEFAULT_LOG_FORMAT, debug=False):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
    logging.root.handlers[0].setFormatter(zope.exceptions.log.Formatter(fmt))


def create_context(env_dir=None, package="nti.spark",
                   ds_env_etc_dir=DATASERVER_ETC_DIR):
    """
    Creates a zope configuration context
    """
    etc = os.path.join(env_dir, 'etc') if env_dir else os.getenv(ds_env_etc_dir)
    etc = os.path.expanduser(etc) if etc else None
    if not etc or not os.path.exists(etc) and not os.path.isdir(etc):
        raise ValueError("Invalid ETC dataserver environment directory",
                         env_dir)
    # create context
    package = dottedname.resolve(package)
    context = config.ConfigurationMachine()
    xmlconfig.registerCommonDirectives(context)
    # load slugs first
    slugs = os.path.join(etc, 'package-includes')
    if os.path.exists(slugs) and os.path.isdir(slugs):
        xmlconfig.include(context,
                          files=os.path.join(slugs, '*.zcml'),
                          package=package)
    # load main package
    context = xmlconfig.file('configure.zcml',
                             package=package,
                             context=context)
    return context


def get_timestamp(timestamp=None, source=None, current=False):
    # parse date
    tstamp = parse_date(timestamp)
    timestamp = make_timestamp(tstamp) if tstamp is not None else None
    # check for current
    tstamp = make_timestamp(time.time()) if current else timestamp
    # check for source
    tstamp = os.path.getmtime(source) if source and not tstamp else tstamp
    return make_timestamp(tstamp) if tstamp is not None else None
