#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import argparse

from zope import component

from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.scripts import create_context
from nti.spark.scripts import configure_logging

logger = __import__('logging').getLogger(__name__)


def process_args(args=None):
    # parse arguments
    arg_parser = argparser.ArgumentParser(description="Produce schema config file for CSV")
    args = arg_parser.parse_args(args)
    
    # configure logging
    configure_logging(debug=args.verbose)

    # create context
    create_context(args.env_dir, "nti.spark")

    # validate
    spark = component.queryUtility(IHiveSparkInstance)
    assert spark is not None, "Must specify a valid Hive/Spark instane"

    # load
    try:
        pass
    finally:
        spark.close()


def main(args=None):
    process_args(args)


if __name__ == '__main__':  # pragma: no cover
    main()
