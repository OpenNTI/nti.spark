#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import numbers
from datetime import date
from datetime import datetime

import isodate

logger = __import__('logging').getLogger(__name__)


def parse_date(data):
    if data is not None:
        for func in (isodate.parse_datetime, isodate.parse_date, float):
            try:
                transformed = func(data)
                if isinstance(transformed, numbers.Number):
                    return datetime.fromtimestamp(transformed)
                elif isinstance(transformed, datetime):
                    return transformed
                elif isinstance(transformed, date):
                    return datetime.combine(transformed, datetime.min.time())
            except Exception:  # pylint: disable=broad-except
                pass
        raise ValueError("Invalid date")
