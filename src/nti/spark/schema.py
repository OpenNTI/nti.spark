#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import codecs
import simplejson

from pyspark.sql.types import DateType
from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import BinaryType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import StructField
from pyspark.sql.types import _infer_type
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

from nti.spark import EXAMPLE
from nti.spark import EXCLUSIONS
from nti.spark import NULLABILITY

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


def construct_schema_example(filename, spark):
    """
    Constructs a single example with all filled in
    columns for a given file
    """
    
    df = spark.read.csv(filename, header=True, inferSchema=True)
    result = {}
    result[EXAMPLE] = {c: None for c in df.columns}
    result[NULLABILITY] = {c: False for c in df.columns}
    for row in df.toLocalIterator():
        for c in df.columns:
            val = getattr(row, c)
            if val is None:
                result[NULLABILITY][c] = True
            if not result[EXAMPLE][c]:
                result[EXAMPLE][c] = val
    return result


def infer_schema(example, nullability=None):
    """
    Infer the schema of a given example.

    This can easily be built off of in the case where
    we're down to singular values and how we want to
    treat them
    """
    if isinstance(example, dict):
        return StructType([StructField(key, 
                           infer_schema(example[key], nullability), 
                           nullability[key] if nullability else True)
                           for key, value in example.iteritems()])
    elif isinstance(example, list):
        if not example:
            raise ValueError("Cannot convert empty list.")
        _type = infer_schema(example[0], nullability)
        for item in example:
            if infer_schema(item, nullability) != _type:
                raise ValueError("Cannot handle multi-type arrays.")
        return ArrayType(_type)
    else:
        if example is None:
            return StringType()
        return _infer_type(example)


def build_exclude_list(example, exclusions):
    """
    Pattern matches on column names to determine if any should
    be excluded on read
    """
    exclusions = exclusions.split(',')
    values = example[EXAMPLE]
    result = []
    for item in exclusions:
        try:
            star_pow = item.index('*')
            if star_pow == 0:
                search = item[1:]
                cols = [x for x in values.keys() if x.endswith(search)]
                result.extend(cols)
            elif star_pow == len(item) - 1:
                search = item[:-1]
                cols = [x for x in values.keys() if x.startswith(search)]
                result.extend(cols)
            else:
                search_begin = item[:star_pow]
                search_end = item[star_pow + 1:]
                cols = [x for x in values.keys() if x.startswith(search_begin) and x.endswith(search_end)]
                result.extend(cols)
        except ValueError:
            result.append(item)
    return result


def save_to_config(filename, spark, config_path, exclusions=None):
    """
    Save the config in a json file at a given location
    """
    example = construct_schema_example(filename, spark)
    if exclusions:
        example[EXCLUSIONS] = build_exclude_list(example, exclusions)
    with codecs.open(config_path, 'w', encoding='utf-8') as fp:
        simplejson.dump(example, fp)


def load_from_config(config_path, cases=None):
    """
    Load a schema from a config file

    Allow an optional cases for special cases
    that cannot be handled automatically
    """

    with codecs.open(config_path, 'r', encoding='utf-8') as fp:
        example = simplejson.load(fp)
    fp.close()
    schema = infer_schema(example[EXAMPLE], example[NULLABILITY])
    if cases:
        nullability = example[NULLABILITY]
        unchanged_fields = [f for f in schema.fields if f.name not in cases.keys()]
        schema.fields = unchanged_fields
        for key, value in cases.iteritems():
            schema.fields.append(StructField(key, value, nullability[key]))
    return schema
