#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import json
import argparse

from zope import component

from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.scripts import create_context
from nti.spark.scripts import configure_logging

logger = __import__('logging').getLogger(__name__)


def process_args(args=None):
    # parse arguments
    arg_parser = argparser.ArgumentParser(description="Produce schema config file for CSV")
    arg_parser.add_argument('-f', '--file', help='File to produce schema')
    arg_parser.add_argument('-e', '--exclude', help='List of patterns of column names to exclude on load')
    arg_parser.add_argument('-o', '--out', help='Directory to write resulting config')
    arg_parser.add_argument('-v', '--verbose', help="Verbose mode",
                            action='store_true', dest='verbose')
    arg_parser.add_argument('-d', '--env_dir', dest='env_dir',
                            help=" Environment root directory")
    args = arg_parser.parse_args(args)

    # configure logging
    configure_logging(debug=args.verbose)

    # create context
    create_context(args.env_dir, "nti.spark")

    # validate
    spark = component.queryUtility(IHiveSparkInstance)
    assert spark is not None, "Must specify a valid Hive/Spark instane"

    # Check file
    assert args.file, "Must provide a file to read."

    # Check out directory
    assert args.out, "Must provide directory to write."

    # load
    try:
        pass
    finally:
        spark.close()


def main(args=None):
    process_args(args)


if __name__ == '__main__':  # pragma: no cover
    main()
