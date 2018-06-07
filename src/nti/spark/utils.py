#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import re
import time
import numbers
from datetime import date
from datetime import datetime

import isodate

import pytz

import six

logger = __import__('logging').getLogger(__name__)


def csv_mode(strict=False):
    return "DROPMALFORMED" if not strict else "FAILFAST"


def fromtimestamp(data):
    if isinstance(data, numbers.Number):
        data = datetime.fromtimestamp(data)
    return data


def parse_date(data):
    if data is not None:
        for func in (isodate.parse_datetime, isodate.parse_date, float):
            try:
                transformed = func(data)
                if isinstance(transformed, numbers.Number):
                    return fromtimestamp(transformed)
                elif isinstance(transformed, datetime):
                    return transformed
                elif isinstance(transformed, date):
                    return datetime.combine(transformed, datetime.min.time())
            except Exception:  # pylint: disable=broad-except
                pass
        raise ValueError("Invalid date")


def parse_date_as_utc(data, is_dst=False):
    data = parse_date(data)
    return pytz.utc.localize(data, is_dst) if data is not None else None


def get_timestamp(timestamp=None):
    if timestamp is None:
        timestamp = date.today()
    if isinstance(timestamp, (date, datetime)):
        timestamp = time.mktime(timestamp.timetuple())
    return int(timestamp)


def text_(s, encoding='utf-8', err='strict'):
    """
    Return a string and unicode version of an object. 
    If the object is an byte sequence it's decoded first

    :param object s: The object to get an unicode representation of.
    :param str encoding: The encoding to be used if ``s`` is a byte sequence
    :param str err: The err handling scheme to be used if ``s`` is a byte sequence
    """
    s = s.decode(encoding, err) if isinstance(s, bytes) else s
    return six.text_type(s) if s is not None else None


def safe_header(header):
    if header:
        # pylint: disable=broad-except
        try:
            header = header.encode('ascii', 'xmlcharrefreplace')
        except Exception:  # pragma: no cover
            pass
        header = re.sub(r'[/<>:;"\\|#?*\s]+', '_', text_(header))
        header = re.sub(r'&', '_', header)
        header = text_(header)
    return header
