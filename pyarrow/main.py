import logging
import os
from uuid import uuid4

import awswrangler as wr
import pandas as pd
from typing import Literal
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as paj
import pyarrow.parquet as pq

tmp_directory = os.getenv("TMP_DIRECTORY", ".")
sort_column = os.getenv("SORT_COLUMN", "time")
destination_bucket = os.getenv("DESTINATION_BUCKET")


def lambda_handler(event: dict, context):
    if logging.getLogger().level == logging.DEBUG:
        # We need to delete 'message' when outputting the event; it conflicts with the 'logging' key of the same name.
        debug_event = event.copy()
        debug_event["original-message"] = debug_event.pop("message", None)
        logging.debug("event", extra=debug_event)

    if destination_bucket is None:
        msg = "missing DESTINATION_BUCKET environment variable"
        logging.error(msg, extra={"type": "app.error"})
        raise ValueError(msg)

    # We only expect to have to deal with one record at a time.
    if len(event["Records"]) > 1:
        msg = (
            f"We only expect a single record per invoke. {len(event['Records'])} seen."
        )
        logging.error(msg, extra={"type": "app.error", "records": event["Records"]})
        raise ValueError(msg)

    # Because of the above we don't need a loop here; but keep it for if we ever remove the 1 record restriction.
    for record in event["Records"]:
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        key = record.get("s3", {}).get("object", {}).get("key")

        if bucket is None or key is None:
            logging.error(
                "Missing bucket or key in event",
                extra={"type": "app.error", "records": event["Records"]},
            )
            raise ValueError("Missing bucket or key in event")

        convert_object_to_parquet(bucket, key)



def get_table_from_s3(source_s3_path: str) -> None | pa.Table:
    tmp_file_path = f"{tmp_directory}/data.ndjson.gz"

    try:
        wr.s3.download(source_s3_path, tmp_file_path)
        table = paj.read_json(tmp_file_path)
        return table
    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)



def sort_table(table: pa.Table) -> tuple[pa.Table, str, tuple[pq.SortingColumn] | None]:
    sorting_columns = None

    if sort_column not in table.schema.names:
        sort_log = (
            f"the designated sorting column - {sort_column} - was not in the table"
        )
    else:
        sort_log = f"ensuring the data is sorted on column '{sort_column}'"
        sort_keys: list[tuple[str, Literal["ascending"]]] = [(sort_column, "ascending")]
        indices = pc.sort_indices(table, sort_keys)
        table = pc.take(table, indices)

        sorting_columns = pq.SortingColumn.from_ordering(table.schema, sort_keys)

    return table, sort_log, sorting_columns


def zstd_compression_available() -> bool:
    try:
        # This will throw an exception if pyarrow is not built with zstd support.
        pa.Codec("zstd")
        return True
    except Exception:
        return False


def convert_object_to_parquet(bucket: str, key: str):
    """Process JSON data from S3, convert to Parquet with nested structure, and save back to S3."""

    source_s3_path = f"s3://{bucket}/{key}"
    logging.info(
        f"Reading from: {source_s3_path}",
        extra={"type": "app.info", "bucket": bucket, "key": key},
    )

    table = get_table_from_s3(source_s3_path)

    if table is None:
        raise ValueError("Unable to load table from S3.")

    # ---

    original_schema = table.schema
    table = convert_datetime_fields(table)

    # ---

    table, sort_log, sorting_columns = sort_table(table)

    # ---

    # The source and destination key match; only the bucket and extension change.
    key_stem = key.split(".", 1)[0]
    key_prefix = f"s3://{destination_bucket}/{key_stem}"

    # ---

    if zstd_compression_available():
        # We prefer zstandard, but it might not be available.
        # It's not included in the AWS Pandas SDK Lambda layer, for example.
        compression = "zstd"
        destination_s3_path = key_prefix + ".zst.parquet"
    else:
        # zstandard is not supported. Falling back to gzip.
        compression = "gzip"
        destination_s3_path = key_prefix + ".gz.parquet"

    # ---

    tmp_file_path = f"{tmp_directory}/{uuid4()}.parquet"

    try:
        pq.write_table(
            table=table,
            where=tmp_file_path,
            compression=compression,
            sorting_columns=sorting_columns,
            data_page_size=1_048_576,  # 1 MB uncompressed pages
            use_deprecated_int96_timestamps=False,  # Avoid deprecated INT96 timestamps
            allow_truncated_timestamps=False,  # Ensure timestamps are not truncated
        )

        wr.s3.upload(tmp_file_path, path=destination_s3_path)

        logging.info(
            f"Written parquet to: {destination_s3_path}",
            extra={
                "type": "app.info",
                "path": destination_s3_path,
                "rows": table.num_rows,
                "compression": compression,
                "sorting": sort_log,
                "schema": {
                    "original": schema_to_json(original_schema),
                    "updated": schema_to_json(table.schema),
                },
            },
        )

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)


def convert_datetime_fields(table: pa.Table) -> pa.Table:
    new_columns = []
    new_names = []

    for field in table.schema:
        new_names.append(field.name)

        if field.name.endswith("_dt") and pa.types.is_string(field.type):
            new_array = pa.array(
                pd.to_datetime(table[field.name], utc=True, format="ISO8601")
            )
            new_columns.append(new_array)
        elif pa.types.is_struct(field.type) and isinstance(
            table[field.name], pa.ChunkedArray
        ):
            new_array = convert_datetime_fields_chunked_array(table[field.name], field)
            new_columns.append(new_array)
        elif pa.types.is_list(field.type) and isinstance(
            table[field.name], pa.ChunkedArray
        ):
            new_array = convert_datetime_fields_chunked_array(table[field.name], field)
            new_columns.append(new_array)
        else:
            new_columns.append(table[field.name])

    # Create new table with updated columns
    return pa.Table.from_arrays(
        names=new_names,
        arrays=new_columns,
    )


def convert_datetime_fields_chunked_array(
    column: pa.ChunkedArray, field: pa.Field
) -> pa.ChunkedArray:
    # ChunkedArray is a fancy pa.Array, made up of chunks. Those chunks all have the same type.

    transformed_chunks = []

    if pa.types.is_struct(column.type):
        for chunk in column.chunks:
            new_chunk = convert_datetime_fields_struct_array(chunk)
            transformed_chunks.append(new_chunk)

    elif pa.types.is_list(column.type):
        for chunk in column.chunks:
            new_chunk = convert_datetime_fields_list_array(chunk)
            transformed_chunks.append(new_chunk)

    else:
        raise ValueError("Unexpected column type")

    return pa.chunked_array(transformed_chunks)


def convert_datetime_fields_struct_array(struct: pa.StructArray) -> pa.StructArray:
    transformed_arrays = []

    for field in struct.type:

        if field.name.endswith("_dt") and pa.types.is_string(field.type):
            new_array = pa.array(
                pd.to_datetime(struct.field(field.name), utc=True, format="ISO8601")
            )
            transformed_arrays.append(new_array)

        elif pa.types.is_struct(field.type):
            new_array = convert_datetime_fields_struct_array(struct.field(field.name))
            transformed_arrays.append(new_array)

        elif pa.types.is_list(field.type):
            # transformed_arrays.append(struct.field(field.name))
            new_array = convert_datetime_fields_list_array(struct.field(field.name))
            transformed_arrays.append(new_array)

        else:
            transformed_arrays.append(struct.field(field.name))

    # We build a new StructArray using the column names of the original,
    # and the new transformed arrays.
    return pa.StructArray.from_arrays(
        names=[field.name for field in struct.type],
        arrays=transformed_arrays,
    )


def convert_datetime_fields_list_array(list_array: pa.ListArray):
    value_type = list_array.type.value_type

    # Get the underlying values array from the ListArray
    values = list_array.flatten()
    offsets = list_array.offsets

    if pa.types.is_struct(value_type):
        # Recursively handle nested struct arrays within the list
        struct_array = pa.StructArray.from_arrays(
            names=[f.name for f in value_type],
            arrays=[values.field(i) for i in range(len(value_type))],
        )
        converted_values = convert_datetime_fields_struct_array(struct_array)
        converted_values = pa.array(converted_values)

    else:
        # Pass through unchanged if not a datetime or nested type
        converted_values = values

    # Reconstruct the ListArray with converted values
    return pa.ListArray.from_arrays(offsets=offsets, values=converted_values)


def schema_to_json(schema) -> dict:
    fields = {}
    for field in schema:
        if pa.types.is_struct(field.type):
            fields[field.name] = schema_to_json(field.type)
        elif pa.types.is_list(field.type):
            fields[field.name] = [
                (
                    schema_to_json(field.type.value_type)
                    if pa.types.is_struct(field.type.value_type)
                    else str(field.type.value_type)
                )
            ]
        else:
            fields[field.name] = str(field.type)
    return fields


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "json-to-parquet-nsmith"},
                    "object": {"key": "findings/finding-unsorted.ndjson.gz"},
                }
            },
        ]
    }

    lambda_handler(event, None)
