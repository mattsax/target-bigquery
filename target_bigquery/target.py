# Copyright (c) 2023 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
"""BigQuery target class."""
import os
import copy
import time
import uuid
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type, Union

from singer_sdk import Sink
from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink, BigQueryBatchJobSink
from target_bigquery.core import BaseBigQuerySink, BaseWorker, BigQueryCredentials, ParType
from target_bigquery.gcs_stage import BigQueryGcsStagingDenormalizedSink, BigQueryGcsStagingSink
from target_bigquery.storage_write import (
    BigQueryStorageWriteDenormalizedSink,
    BigQueryStorageWriteSink,
)
from target_bigquery.streaming_insert import (
    BigQueryStreamingInsertDenormalizedSink,
    BigQueryStreamingInsertSink,
)

if TYPE_CHECKING:
    from multiprocessing import Process, Queue
    from multiprocessing.connection import Connection

# Defaults for target worker pool parameters
MAX_WORKERS = 15
"""Maximum number of workers to spawn."""
MAX_JOBS_QUEUED = 30
"""Maximum number of jobs placed in the global queue to avoid memory overload."""
WORKER_CAPACITY_FACTOR = 5
"""Jobs enqueued must exceed the number of active workers times this number."""
WORKER_CREATION_MIN_INTERVAL = 5
"""Minimum time between worker creation attempts."""

class TargetBigQuery(Target):
    """Target for BigQuery."""

    _MAX_RECORD_AGE_IN_MINUTES = 5.0
    
    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "credentials_path",
            th.StringType,
            description="The path to a gcp credentials json file.",
        ),
        th.Property(
            "credentials_json",
            th.StringType,
            description="A JSON string of your service account JSON file.",
        ),
        th.Property(
            "project",
            th.StringType,
            description="The target GCP project to materialize data into.",
            required=True,
        ),
        th.Property(
            "dataset",
            th.StringType,
            description="The target dataset to materialize data into.",
            required=True,
        ),
        th.Property(
            "location",
            th.StringType,
            description="The target dataset/bucket location to materialize data into.",
            default="US",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The maximum number of rows to send in a single batch or commit.",
            default=500,
        ),
        th.Property(
            "fail_fast",
            th.BooleanType,
            description="Fail the entire load job if any row fails to insert.",
            default=True,
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            description="Default timeout for batch_job and gcs_stage derived LoadJobs.",
            default=600,
        ),
        th.Property(
            "denormalized",
            th.BooleanType,
            description=(
                "Determines whether to denormalize the data before writing to BigQuery. A false"
                " value will write data using a fixed JSON column based schema, while a true value"
                " will write data using a dynamic schema derived from the tap."
            ),
            default=False,
        ),
        th.Property(
            "method",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "storage_write_api",
                        "batch_job",
                        "gcs_stage",
                        "streaming_insert",
                    ],
                }
            ),
            description="The method to use for writing to BigQuery.",
            default="storage_write_api",
            required=True,
        ),
        th.Property(
            "generate_view",
            th.BooleanType,
            description=(
                "Determines whether to generate a view based on the SCHEMA message parsed from the"
                " tap. Only valid if denormalized=false meaning you are using the fixed JSON column"
                " based schema."
            ),
            default=False,
        ),
        th.Property(
            "bucket",
            th.StringType,
            description="The GCS bucket to use for staging data. Only used if method is gcs_stage.",
        ),
        th.Property(
            "partition_granularity",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "year",
                        "month",
                        "day",
                        "hour",
                    ],
                }
            ),
            default="month",
            description="The granularity of the partitioning strategy. Defaults to month.",
        ),
        th.Property(
            "cluster_on_key_properties",
            th.BooleanType,
            default=False,
            description=(
                "Determines whether to cluster on the key properties from the tap. Defaults to"
                " false. When false, clustering will be based on _sdc_batched_at instead."
            ),
        ),
        th.Property(
            "column_name_transforms",
            th.ObjectType(
                th.Property(
                    "lower",
                    th.BooleanType,
                    default=False,
                    description="Lowercase column names",
                ),
                th.Property(
                    "quote",
                    th.BooleanType,
                    default=False,
                    description="Quote columns during DDL generation",
                ),
                th.Property(
                    "add_underscore_when_invalid",
                    th.BooleanType,
                    default=False,
                    description="Add an underscore when a column starts with a digit",
                ),
                th.Property(
                    "snake_case",
                    th.BooleanType,
                    default=False,
                    description="Convert columns to snake case",
                ),
            ),
            description=(
                "Accepts a JSON object of options with boolean values to enable them. The available"
                " options are `quote` (quote columns in DDL), `lower` (lowercase column names),"
                " `add_underscore_when_invalid` (add underscore if column starts with digit), and"
                " `snake_case` (convert to snake case naming). For fixed schema, this transform"
                " only applies to the generated view if enabled."
            ),
            required=False,
        ),
        th.Property(
            "options",
            th.ObjectType(
                th.Property(
                    "storage_write_batch_mode",
                    th.BooleanType,
                    default=False,
                    description=(
                        "By default, we use the default stream (Committed mode) in the"
                        " storage_write_api load method which results in streaming records which"
                        " are immediately available and is generally fastest. If this is set to"
                        " true, we will use the application created streams (Committed mode) to"
                        " transactionally batch data on STATE messages and at end of pipe."
                    ),
                ),
                th.Property(
                    "process_pool",
                    th.BooleanType,
                    default=False,
                    description=(
                        "By default we use an autoscaling threadpool to write to BigQuery. If set"
                        " to true, we will use a process pool."
                    ),
                ),
                th.Property(
                    "max_workers",
                    th.IntegerType,
                    required=False,
                    description=(
                        "By default, each sink type has a preconfigured max worker pool limit."
                        " This sets an override for maximum number of workers in the pool."
                    ),
                ),
            ),
            description=(
                "Accepts a JSON object of options with boolean values to enable them. These are"
                " more advanced options that shouldn't need tweaking but are here for flexibility."
            ),
        ),
        th.Property(
            "upsert",
            th.CustomType(
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                }
            ),
            default=False,
            description=(
                "Determines if we should upsert. Defaults to false. A value of true will write to a"
                " temporary table and then merge into the target table (upsert). This requires the"
                " target table to be unique on the key properties. A value of false will write to"
                " the target table directly (append). A value of an array of strings will evaluate"
                " the strings in order using fnmatch. At the end of the array, the value of the"
                " last match will be used. If not matched, the default value is false (append)."
            ),
        ),
        th.Property(
            "overwrite",
            th.CustomType(
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                }
            ),
            default=False,
            description=(
                "Determines if the target table should be overwritten on load. Defaults to false. A"
                " value of true will write to a temporary table and then overwrite the target table"
                " inside a transaction (so it is safe). A value of false will write to the target"
                " table directly (append). A value of an array of strings will evaluate the strings"
                " in order using fnmatch. At the end of the array, the value of the last match will"
                " be used. If not matched, the default value is false. This is mutually exclusive"
                " with the `upsert` option. If both are set, `upsert` will take precedence."
            ),
        ),
        th.Property(
            "dedupe_before_upsert",
            th.CustomType(
                {
                    "anyOf": [
                        {"type": "boolean"},
                        {"type": "array", "items": {"type": "string"}},
                    ]
                }
            ),
            default=False,
            description=(
                "This option is only used if `upsert` is enabled for a stream. The selection"
                " criteria for the stream's candidacy is the same as upsert. If the stream is"
                " marked for deduping before upsert, we will create a _session scoped temporary"
                " table during the merge transaction to dedupe the ingested records. This is useful"
                " for streams that are not unique on the key properties during an ingest but are"
                " unique in the source system. Data lake ingestion is often a good example of this"
                " where the same unique record may exist in the lake at different points in time"
                " from different extracts."
            ),
        ),
        th.Property(
            "schema_resolver_version",
            th.IntegerType,
            default=1,
            description=(
                "The version of the schema resolver to use. Defaults to 1. Version 2 uses JSON as a"
                " fallback during denormalization. This only has an effect if denormalized=true"
            ),
            allowed_values=[1, 2],
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: Optional[str] = None) -> Type[BaseBigQuerySink]:
        """Returns the sink class to use for a given stream based on user config."""
        _ = stream_name
        method, denormalized = self.config.get("method", "storage_write_api"), self.config.get(
            "denormalized", False
        )
        if method == "batch_job":
            if denormalized:
                return BigQueryBatchJobDenormalizedSink
            return BigQueryBatchJobSink
        elif method == "streaming_insert":
            if denormalized:
                return BigQueryStreamingInsertDenormalizedSink
            return BigQueryStreamingInsertSink
        elif method == "gcs_stage":
            if denormalized:
                return BigQueryGcsStagingDenormalizedSink
            return BigQueryGcsStagingSink
        elif method == "storage_write_api":
            if denormalized:
                return BigQueryStorageWriteDenormalizedSink
            return BigQueryStorageWriteSink
        raise ValueError(f"Unknown method: {method}")
