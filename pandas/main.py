import logging
import awswrangler as wr
import pandas as pd
import os
import pyarrow as pa


sort_column = os.getenv("SORT_COLUMN", 'time')
destination_bucket = os.getenv("DESTINATION_BUCKET", 'json-to-parquet-nsmith')

def lambda_handler(event: dict, context):
    if logging.getLogger().level == logging.DEBUG:
        # We need to delete 'message' when outputting the event; it conflicts with the 'logging' key of the same name.
        debug_event = event.copy()
        debug_event['original-message'] = debug_event.pop("message", None)
        logging.debug('event', extra=debug_event)

    if destination_bucket is None:
        msg = "missing DESTINATION_BUCKET environment variable"
        logging.error(msg, extra={'type': 'app.error'})
        raise ValueError(msg)

    # We only expect to have to deal with one record at a time.
    if len(event["Records"]) > 1:
        msg = f"we only expect a single record to be passed per invoke. {len(event["Records"])} seen"
        logging.error(msg,
            extra={
                'type': 'app.error',
                'records': event["Records"]
            }
        )
        raise ValueError(msg)

    # Because of the above we don't need a loop here; but keep it for if we ever remove the 1 record restriction.
    for record in event["Records"]:
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        key = record.get("s3", {}).get("object", {}).get("key")

        if bucket is None or key is None:
            logging.error("Missing bucket or key in event",
                extra={
                    'type': 'app.error',
                    'records': event["Records"]
                }
            )
            raise ValueError("Missing bucket or key in event")

        convert_object_to_parquet(bucket, key)


def convert_object_to_parquet(bucket: str, key: str):
    """Process JSON data from S3, convert to Parquet with nested structure, and save back to S3."""

    source_s3_path = f"s3://{bucket}/{key}"
    logging.info(f"Reading from: {source_s3_path}", extra={
        'type': 'app.info',
        'bucket': bucket,
        'key': key
    })

    # Gets the object as a dataframe.
    # A Process of mapping datetime fields to the correct type is done at the same time.
    df = get_dataframe_and_map_datetimes(source_s3_path)

    if sort_column not in df.columns:
        sort_log = f"the designated sorting column, {sort_column}, was not in the dataframe"
    elif df[sort_column].is_monotonic_increasing:
        sort_log = f"data was already sorted on column {sort_column}"
    else:
        df.sort_values(by=sort_column, inplace=True)
        sort_log = f"data was sorted by column {sort_column}"


    # The source and destination key match exactly; only the bucket changes.
    key_stem = key.split('.', 1)[0]
    key_prefix = f"s3://{destination_bucket}/{key_stem}"

    try:
        # We prefer zstandard, but it might not be available.
        # It's not included in the AWS Pandas SDK Lambda layer, for example.

        # This will throw an exception if pyarrow is not build with zstd support.
        pa.Codec("zstd")

        compression = "zstd"
        destination_s3_path = key_prefix + ".zst.parquet"
    except Exception:
        # zstandard is not supported. Falling back to gzip.
        compression = "gzip"
        destination_s3_path = key_prefix + ".gz.parquet"

    logging.info(f"Writing to: {destination_s3_path}", extra={
        'type': 'app.info',
        'path': destination_s3_path,
        'rows': df.shape[0],
        'compression': compression,
        'sorting': sort_log,
    })

    # Write Parquet with optimized settings and nested structure
    wr.s3.to_parquet(
        df=df,
        path=destination_s3_path,
        compression=compression,
        pyarrow_additional_kwargs={
            "data_page_size": 1_048_576,  # 1 MB uncompressed pages
            "use_deprecated_int96_timestamps": False,
            "allow_truncated_timestamps": False,
        },
    )


def get_dataframe_and_map_datetimes(source_s3_path):
    """
    Reads JSON data from the specified S3 path, processes it in chunks, converts datetime
    fields within the records, and returns a consolidated pandas DataFrame containing
    the processed records.

    The function utilizes AWS Wrangler to read JSON data in chunks from an S3 bucket. Each
    chunk is passed through datetime conversion logic and then appended to a list of processed
    records. Once all data is processed, it creates and returns a pandas DataFrame containing
    the finalized records.

    :param source_s3_path: The S3 path from which the JSON data will be read.
    :type source_s3_path: str
    :return: A pandas DataFrame containing the processed records with datetime fields mapped.
    :rtype: pandas.DataFrame
    """
    df = pd.DataFrame()
    for chunk in wr.s3.read_json(source_s3_path, lines=True, chunksize=5_000):
        mapped_data = to_datetime(chunk)
        df = pd.concat([df, pd.DataFrame(mapped_data)], ignore_index=True)

    return df

def get_dataframe_and_map_datetimes_old(source_s3_path):
    """
    Reads JSON data from the specified S3 path, processes it in chunks, converts datetime
    fields within the records, and returns a consolidated pandas DataFrame containing
    the processed records.

    The function utilizes AWS Wrangler to read JSON data in chunks from an S3 bucket. Each
    chunk is passed through datetime conversion logic and then appended to a list of processed
    records. Once all data is processed, it creates and returns a pandas DataFrame containing
    the finalized records.

    :param source_s3_path: The S3 path from which the JSON data will be read.
    :type source_s3_path: str
    :return: A pandas DataFrame containing the processed records with datetime fields mapped.
    :rtype: pandas.DataFrame
    """
    processed_records = []
    for chunk in wr.s3.read_json(source_s3_path, lines=True, chunksize=5_000):
        processed_records.extend(to_datetime(chunk))

    return pd.DataFrame(processed_records)

def to_datetime(df: pd.DataFrame):
    """
    Convert a pandas DataFrame with JSON-like fields to a list of nested dictionaries,
    ensuring proper handling of datetime fields.

    This function normalises JSON-like fields in the DataFrame, identifies columns that
    end with '_dt' to indicate datetime fields, and converts those columns to datetime
    objects in UTC. It then converts the DataFrame into a list of dictionaries with a
    nested structure using dot notation.

    This seems to be the most efficient way to covert datetimes, given that all we know is the key will end _dt.

    :param df: A pandas DataFrame containing JSON-like fields with possible datetime
        columns ending in '_dt'.
    :type df: pd.DataFrame
    :return: A list of dictionaries with nested structure and datetime values converted
        to UTC as needed.
    :rtype: list[dict]
    """

    # Flatten JSON-like fields
    df = pd.json_normalize(df.to_dict(orient="records"))

    # Identify `_dt` columns
    dt_columns = [col for col in df.columns if col.endswith('_dt')]

    # Convert `_dt` fields in bulk
    df[dt_columns] = df[dt_columns].apply(pd.to_datetime, errors='coerce', utc=True)

    # Convert DataFrame back to a JSON-like list of dictionaries
    records = df.to_dict(orient="records")

    # Convert dot-notation back into a nested dictionary structure
    for idx, record in enumerate(records):
        records[idx] = dot_to_nested_dict(record)

    return records

def dot_to_nested_dict(dot_dict: dict):
    """
    Converts a dictionary with keys containing dot-notation into a nested dictionary structure.

    This function takes a dictionary with keys formatted in dot-notation (e.g., "key1.key2.key3")
    and restructures it into a nested dictionary. For example, a key-value pair
    like "key1.key2: value" will be converted into a nested dictionary where "key1" is
    the outer key, "key2" is an inner key, and "value" is the corresponding value.

    :param dot_dict: Dictionary where keys use dot-notation to indicate levels of nesting.
    :type dot_dict: dict
    :return: A new dictionary with keys and values restructured into a nested format
        based on the dot-notation in the input dictionary.
    :rtype: dict
    """
    nested_dict = {}
    for key, value in dot_dict.items():
        parts = key.split('.')
        d = nested_dict
        for part in parts[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value
    return nested_dict

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    event = {'Records': [
        {'s3': {'bucket': {'name': 'json-to-parquet-nsmith'}, 'object': {'key': 'findings/finding-sorted.ndjson.gz'}}},
    ]}

    lambda_handler(event, None)
