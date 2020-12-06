# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
PostgreSQL to GCS operator.
"""

import datetime
from decimal import Decimal
import time

import sys

from airflow.utils.decorators import apply_defaults
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

PY3 = sys.version_info[0] == 3


class PostgresToCleanGoogleCloudStorageOperator(PostgresToGoogleCloudStorageOperator):
    @apply_defaults
    def __init__(self,
                postgres_conn_id='postgres_default',
                transform_functions={},
                *args,
                 **kwargs):
        super(PostgresToCleanGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.transform_functions = transform_functions


    def transform_value(self, name, value):
        if name in self.transform_functions:
            fun = self.transform_functions[name]
            return fun(value)
        
        return value

    def convert_types(self, schema, col_type_dict, row):
        row_clean = [
            self.transform_value(name, value)
            for name, value in zip(schema, row)
        ]

        """Convert values from DBAPI to output-friendly formats."""
        return [
            self.convert_type(value, col_type_dict.get(name))
            for name, value in zip(schema, row_clean)
        ]
