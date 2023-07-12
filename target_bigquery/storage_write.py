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
"""BigQuery Storage Write Sink.
Throughput test: 11m 0s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

import orjson
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.protobuf import json_format
from proto import Message

if TYPE_CHECKING:
    from target_bigquery.target import TargetBigQuery

import logging

from target_bigquery.core import BaseBigQuerySink, Denormalized, storage_client_factory
from target_bigquery.proto_gen import proto_schema_factory_v2

logger = logging.getLogger(__name__)

def generate_template(message: Type[Message]):
    """Generate a template for the storage write API from a proto message class."""
    from google.protobuf import descriptor_pb2

    request_template, proto_schema, proto_descriptor, proto_data = (
        types.AppendRowsRequest(),
        types.ProtoSchema(),
        descriptor_pb2.DescriptorProto(),
        types.AppendRowsRequest.ProtoData(),
    )
    message.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data
    return request_template


class BigQueryStorageWriteSink(BaseBigQuerySink):
    def __init__(
        self,
        target: "TargetBigQuery",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.write_client: BigQueryWriteClient = storage_client_factory(self._credentials)
        self.parent = self.write_client.table_path(
            self.table.project,
            self.table.dataset,
            self.table.name,
        )

        self.request_template = generate_template(self.proto_schema)
        write_stream = types.WriteStream()
        write_stream.type_ = types.WriteStream.Type.PENDING
        self.write_stream = self.write_client.create_write_stream(
            parent=self.parent,
            write_stream=write_stream
        )
        self.request_template.write_stream = self.write_stream.name
        self.append_rows_stream = writer.AppendRowsStream(self.write_client, self.request_template)
        self.offset = 0
        if os.environ.get("LOGLEVEL", "false").upper() == "DEBUG":
            bidi_logger = logging.getLogger("google.api_core.bidi")
            bidi_logger.setLevel(logging.DEBUG)

    @property
    def proto_schema(self) -> Type[Message]:
        if not hasattr(self, "_proto_schema"):
            self._proto_schema = proto_schema_factory_v2(
                self.table.get_resolved_schema(self.apply_transforms)
            )
        return self._proto_schema

    def start_batch(self, context: Dict[str, Any]) -> None:
        # self.stream_name = self.write_stream.name
        self.proto_rows = types.ProtoRows()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.proto_rows.serialized_rows.append(
            json_format.ParseDict(record, self.proto_schema()).SerializeToString()
        )

    def process_batch(self, context: Dict[str, Any]) -> None:
        request = types.AppendRowsRequest()
        request.offset = self.offset
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = self.proto_rows
        request.proto_rows = proto_data
        response_future_1 = self.append_rows_stream.send(request)
        self.logger.info(response_future_1.result())
        self.offset += len(self.proto_rows.serialized_rows)

    def commit_streams(self) -> None:
        self.append_rows_stream.close()
        self.write_client.finalize_write_stream(name=self.write_stream.name)
        batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
        batch_commit_write_streams_request.parent = self.parent
        batch_commit_write_streams_request.write_streams = [self.write_stream.name]
        self.write_client.batch_commit_write_streams(batch_commit_write_streams_request)
        self.logger.info(f"Writes to streams: '{self.stream_name}' have been committed.")

    def clean_up(self) -> None:
        self.commit_streams()
        super().clean_up()

    def pre_state_hook(self) -> None:
        self.commit_streams()


class BigQueryStorageWriteDenormalizedSink(Denormalized, BigQueryStorageWriteSink):
    pass
