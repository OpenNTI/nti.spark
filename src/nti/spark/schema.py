#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import codecs
from datetime import date
from datetime import datetime
from collections import Mapping

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

import simplejson

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

from nti.spark import ORDER
from nti.spark import EXAMPLE
from nti.spark import EXCLUSIONS
from nti.spark import NULLABILITY

from nti.spark.utils import csv_mode
from nti.spark.utils import safe_header

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
    result = {}
    spark = getattr(spark, 'hive', spark)
    df = spark.read.csv(filename, header=True, inferSchema=True)
    safe_columns = [safe_header(c) for c in df.columns]
    result[ORDER] = safe_columns
    result[EXAMPLE] = {c: None for c in safe_columns}
    result[NULLABILITY] = {c: False for c in safe_columns}
    for row in df.toLocalIterator():
        for c in df.columns:
            safe = safe_header(c)
            val = getattr(row, c)
            if val is None:
                result[NULLABILITY][safe] = True
            if not result[EXAMPLE][safe] or result[EXAMPLE][safe] == 0:
                result[EXAMPLE][safe] = val
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
                                       infer_schema(value, nullability),
                                       nullability[key] if nullability else True)
                           for key, value in example.items()])
    elif isinstance(example, list):
        assert example, "Cannot convert empty list."
        type_ = infer_schema(example[0], nullability)
        for item in example:
            infered = infer_schema(item, nullability)
            assert infered == type_, "Cannot handle multi-type arrays."
        return ArrayType(type_)
    else:
        if example is None:
            return StringType()
        return _infer_type(example)


def build_exclude_list(example, exclusions):
    """
    Pattern matches on column names to determine if any should
    be excluded on read
    """
    result = []
    values = example[EXAMPLE]
    exclusions = exclusions.split(',')
    for item in exclusions:
        try:
            star_pow = item.index('*')
            if star_pow == 0:
                search = item[1:]
                cols = [x for x in values if x.endswith(search)]
                result.extend(cols)
            elif star_pow == len(item) - 1:
                search = item[:-1]
                cols = [x for x in values if x.startswith(search)]
                result.extend(cols)
            elif star_pow:
                search_begin = item[:star_pow]
                search_end = item[star_pow + 1:]
                cols = [
                    x for x in values if x.startswith(search_begin) and x.endswith(search_end)
                ]
                result.extend(cols)
        except ValueError:
            result.append(item)
    return result


def serialize_json(obj):  # pragma: no cover
    """
    Handle non auto-serializable objects
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s is not JSON serializable" % type(obj))


def save_to_config(filename, spark, config_path, exclusions=None):
    """
    Save the config in a json file at a given location
    """
    example = construct_schema_example(filename, spark)
    if exclusions:
        example[EXCLUSIONS] = build_exclude_list(example, exclusions)
    with codecs.open(config_path, 'w', encoding='utf-8') as fp:
        simplejson.dump(example, fp, indent=4, default=serialize_json)


def load_from_config(config_path, cases=None):
    """
    Load a schema from a config file

    Allow an optional cases for special cases
    that cannot be handled automatically
    """
    assert cases is None or isinstance(cases, Mapping)
    with codecs.open(config_path, 'r', encoding='utf-8') as fp:
        example = simplejson.load(fp)
    config_schema = infer_schema(example[EXAMPLE], example[NULLABILITY])
    if cases:
        nullability = example[NULLABILITY]
        unchanged_fields = [
            f for f in config_schema.fields if f.name not in cases
        ]
        config_schema.fields = unchanged_fields
        for key, value in cases.items():
            config_schema.fields.append(StructField(key, value, nullability[key]))
    # Order of the items in the schema is important
    order_dict = {field.name: field for field in config_schema.fields}
    ordered_fields = [order_dict[key] for key in example[ORDER]]
    config_schema.fields = ordered_fields
    exclusions = example[EXCLUSIONS] if EXCLUSIONS in example.keys() else None
    return config_schema, exclusions


def adhere_to_file(schema, filename, spark):
    spark = getattr(spark, 'sparkContext', spark)
    file_headers = spark.textFile(filename)
    # Check for empty file
    assert not file_headers.isEmpty(), "Cannot read empty file."
    # Get the first line
    file_headers = file_headers.take(1).pop().split(',')
    file_headers = [safe_header(h) for h in file_headers]
    schema_headers = [f.name for f in schema.fields]
    # Both header lists should look the same independent of order
    matching_headers = all([h in schema_headers for h in file_headers])
    matching_lengths = len(file_headers) == len(schema_headers)
    # Fail hard if missing column
    assert matching_headers and matching_lengths, "File missing required columns."
    # Reorder the schema fields to match the file
    order_dict = {field.name: field for field in schema.fields}
    ordered_fields = [order_dict[key] for key in file_headers]
    schema.fields = ordered_fields
    return schema


def read_file_with_config(filename, config_path, spark, 
                          cases=None, strict=False,
                          clean=None, adhere=False):
    """
    Read a CSV file with schema saved to a JSON config
    """
    hive = getattr(spark, 'hive', spark)
    cfg_schema, exclusions = load_from_config(config_path, cases)
    if adhere:
        cfg_schema = adhere_to_file(cfg_schema, filename, hive)
    data_frame = hive.read.csv(filename, header=True,
                               mode=csv_mode(strict),
                               schema=cfg_schema)
    if exclusions:
        data_frame = data_frame.drop(*exclusions)
    if clean is not None and callable(clean):   # pragma: no cover
        data_frame = clean(data_frame)
    return data_frame
