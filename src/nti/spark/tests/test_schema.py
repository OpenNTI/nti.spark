#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods,inherit-non-class

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_entries
from hamcrest import has_property
from hamcrest import has_properties

import os
import shutil
import tempfile

from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from zope import component
from zope import interface

from nti.spark import EXAMPLE
from nti.spark import NULLABILITY

from nti.schema.field import Int
from nti.schema.field import Bool
from nti.schema.field import Date
from nti.schema.field import Text
from nti.schema.field import Number
from nti.schema.field import Object
from nti.schema.field import DateTime
from nti.schema.field import ValidURI
from nti.schema.field import ValidBytes
from nti.schema.field import TextLine
from nti.schema.field import IndexedIterable
from nti.schema.field import DecodingValidTextLine as ValidTextLine

from nti.spark.interfaces import IHiveSparkInstance

from nti.spark.schema import infer_schema
from nti.spark.schema import save_to_config
from nti.spark.schema import load_from_config
from nti.spark.schema import to_pyspark_schema
from nti.spark.schema import build_exclude_list
from nti.spark.schema import construct_schema_example

from nti.spark.tests import SparkLayerTest


class ICategory(interface.Interface):
    id = Int(title=u"The catagory id")
    id.setTaggedValue('__external_accept_id__', True)

    name = ValidTextLine(title=u"The catagory name")


class IGroup(interface.Interface):
    id = Int(title=u"The group id")
    id.setTaggedValue('__external_accept_id__', True)

    name = ValidTextLine(title=u"The group name")


class IOrganization(interface.Interface):

    id = Int(title=u"The id of the organization")
    id.setTaggedValue('__external_accept_id__', True)

    short_name = ValidTextLine(title=u"The organization short name",
                               max_length=300)

    long_name = ValidTextLine(title=u"The organization long name",
                              max_length=500)

    created_at = DateTime(title=u"The organization creation date")

    is_disabled = Bool(title=u"Enabled flag", default=True)

    description = Text(title=u"The organization description",
                       required=False)

    renewed_at = DateTime(title=u"The organization renewed date",
                          required=False)

    alternate_id = Int(title=u"The organization alternate id",
                       required=False)

    member_count = Int(title=u"The organization number of members",
                       required=False,
                       default=0)

    pic_url = ValidURI(title=u"The organization picture",
                       required=False)

    umbrella_id = Int(title=u"The organization umbrella id",
                      required=False)

    keywords = ValidTextLine(title=u"The organization keywords",
                             required=False,
                             max_length=2048)

    cost = Number(title=u"the cost", required=False)

    marked = Date(title=u"the marked", required=False)

    data = ValidBytes(title=u"the data", required=False)

    category = Object(ICategory, required=False,
                      title=u"The organization category")

    groups = IndexedIterable(title=u"A list of groups within the organization",
                             min_length=0,
                             required=False,
                             value_type=Object(IGroup, title=u"The group"))

    account_ids = IndexedIterable(title=u"The id(s) of account(s) to act on",
                                  min_length=0,
                                  required=False,
                                  value_type=Int(title=u"The id"))

    foo = TextLine(title=u"to ignore", required=False)


class TestSchema(SparkLayerTest):

    def test_schema(self):
        schema = to_pyspark_schema(IOrganization, 'foo')
        assert_that(schema.json(), is_not(none()))
        assert_that(schema, has_length(18))

        field = schema['id']
        assert_that(field,
                    has_properties('nullable', False,
                                   'dataType', is_(LongType)))

        field = schema['category']
        assert_that(field,
                    has_properties('nullable', True,
                                   'dataType', is_(StructType)))

        field = schema['account_ids']
        assert_that(field,
                    has_property('dataType', is_(ArrayType)))
        assert_that(field,
                    has_property('dataType',
                                 has_property('elementType', is_(LongType))))

    @property
    def test_file(self):
        path = os.path.join(os.path.dirname(__file__),
                            "data", "test_file.csv")
        return path

    def test_construct(self):
        spark = component.getUtility(IHiveSparkInstance).hive
        result_dict = construct_schema_example(self.test_file, spark)
        assert_that(result_dict[EXAMPLE], has_entries('COL1', True,
                                                      'COL2', 'Austin',
                                                      'COL3', 468,
                                                      'COL4', None))
        assert_that(result_dict[NULLABILITY], has_entries('COL1', True,
                                                          'COL2', True,
                                                          'COL3', True,
                                                          'COL4', True))

    def test_infer_schema(self):
        spark = component.getUtility(IHiveSparkInstance).hive
        example = construct_schema_example(self.test_file, spark)
        schema = infer_schema(example[EXAMPLE], example[NULLABILITY])
        assert_that(schema.fields, has_length(4))

        # Test nested dictionaries
        example[EXAMPLE]["COL5"] = {"COL6": 6, "COL7": 'SomeString'}
        example[NULLABILITY]["COL5"] = False
        example[NULLABILITY]["COL6"] = False
        example[NULLABILITY]["COL7"] = False
        schema = infer_schema(example[EXAMPLE], example[NULLABILITY])
        assert_that(schema.fields, has_length(5))
        added_field = [f for f in schema.fields if f.name == "COL5"].pop()
        assert_that(added_field.dataType.fields, has_length(2))

        # Test list
        example[EXAMPLE]["COL8"] = ['Austin', 'Graham']
        example[NULLABILITY]["COL8"] = False
        schema = infer_schema(example[EXAMPLE], example[NULLABILITY])
        added_field = [f for f in schema.fields if f.name == "COL8"].pop()
        assert_that(added_field.dataType, ArrayType(StringType()))

    def test_exclude_list(self):
        spark = component.getUtility(IHiveSparkInstance).hive
        example = construct_schema_example(self.test_file, spark)
        exclude_list = build_exclude_list(example, "COL*")
        assert_that(exclude_list, has_length(4))
        exclude_list = build_exclude_list(example, "*COL1")
        assert_that(exclude_list, has_length(1))
        exclude_list = build_exclude_list(example, "COL*1")
        assert_that(exclude_list, has_length(1))
        # coverage
        with self.assertRaises(ValueError):
            build_exclude_list(example, "COL")

    def test_save_load_config(self):
        spark = component.getUtility(IHiveSparkInstance).hive
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, 'test.json')
        try:
            save_to_config(self.test_file, spark, path, "COL*")
            assert_that(os.path.exists(path), True)
            schema = load_from_config(path, cases={"COL4": TimestampType()})
            assert_that(schema.fields[-1].dataType, TimestampType())
        finally:
            shutil.rmtree(tmpdir)
