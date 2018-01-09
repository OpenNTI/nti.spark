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
from hamcrest import has_property
from hamcrest import has_properties

from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StructType

from zope import interface

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

from nti.spark.schema import to_pyspark_schema

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
