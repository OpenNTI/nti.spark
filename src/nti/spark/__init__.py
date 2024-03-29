#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

#: Default log level
DEFAULT_LOG_LEVEL = u'ERROR'

#: Default user data location
DEFAULT_LOCATION = u'/user/hive/warehouse'

#: Partion key
PARTITION_KEY = u"partition"

#: Partition information
PARITION_INFORMATION = u"# Partition Information"

#: Timestamp Column
TIMESTAMP = "tstamp"

#: Timestamp Column Type
TIMESTAMP_TYPE = 'bigint'

#: ORC Storage format
ORC = 'ORC'

#: Parquet storage format
PARQUET = 'PARQUET'

#: Default storage format
DEFAULT_STORAGE_FORMAT = ORC

#: Example JSON entry
EXAMPLE = "example"

#: Nullability JSON entry
NULLABILITY = "nullability"

#: Exlusion JSON entry
EXCLUSIONS = "exclusions"

#: Order JSON entry
ORDER = "order"

#: Spark warehouse
SPARK_WAREHOUSE = "spark-warehouse"
