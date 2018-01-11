#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyspark.sql.types import DateType
from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import BinaryType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import StructField
from pyspark.sql.types import TimestampType

from zope import schema

from zope.schema.interfaces import IInt
from zope.schema.interfaces import IURI
from zope.schema.interfaces import IBool
from zope.schema.interfaces import IDate
from zope.schema.interfaces import IText
from zope.schema.interfaces import IFloat
from zope.schema.interfaces import IBytes
from zope.schema.interfaces import IObject
from zope.schema.interfaces import IChoice
from zope.schema.interfaces import IDatetime
from zope.schema.interfaces import ISequence
from zope.schema.interfaces import ITextLine
from zope.schema.interfaces import IBytesLine

logger = __import__('logging').getLogger(__name__)


def to_pyspark_type(field):
    result = None
    if     IURI.providedBy(field) \
        or IText.providedBy(field) \
        or IChoice.providedBy(field) \
        or ITextLine.providedBy(field):
        result = StringType()
    elif   IBytes.providedBy(field) \
        or IBytesLine.providedBy(field):
        result = BinaryType()
    elif IInt.providedBy(field):
        result = LongType()
    elif IFloat.providedBy(field):
        result = DoubleType()
    elif IDate.providedBy(field):
        result = DateType()
    elif IDatetime.providedBy(field):
        result = TimestampType()
    elif IBool.providedBy(field):
        result = BooleanType()
    return result


def to_pyspark_schema(iface, *ignore):
    result = []
    for name in schema.getFieldNamesInOrder(iface):
        if name in ignore:
            continue
        sql_type = None
        field = iface[name]
        required = getattr(field, 'required', True)
        if ISequence.providedBy(field):
            value_type = field.value_type
            if IObject.providedBy(value_type):
                type_ = to_pyspark_schema(value_type.schema)
            else:
                type_ = to_pyspark_type(value_type)
            if type_ is not None:
                sql_type = ArrayType(type_, False)
        elif IObject.providedBy(field):
            sql_type = to_pyspark_schema(field.schema)
        else:
            sql_type = to_pyspark_type(field)
        if sql_type is not None:
            result.append(StructField(name, sql_type, not required))
    result = StructType(result) if result else None
    return result
