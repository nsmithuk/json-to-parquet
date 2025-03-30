import uuid
import gzip
from datetime import datetime, timezone
import json
import random
import shutil
import awswrangler

timestamp = int(datetime.now().timestamp() * 1000)

def generate_random_arn(service="lambda", region="eu-west-2", account_id=None, resource="function"):
    """Generate a random AWS ARN."""
    if account_id is None:
        account_id = random.randint(100000000000, 999999999999)  # Random AWS account ID
    resource_name = f"RandomLambdaFunction-{uuid.uuid4().hex[:8]}"  # Random Lambda function name
    return f"arn:aws:{service}:{region}:{account_id}:{resource}:{resource_name}"


def generate_randomized_json(index):

    global timestamp
    timestamp = timestamp + 1000
    timestamp_iso = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    timestamp2 = timestamp - random.randint(60, 3600)
    timestamp_iso2 = datetime.fromtimestamp(timestamp2 / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    timestamp3 = timestamp - random.randint(60, 3600)
    timestamp_iso3 = datetime.fromtimestamp(timestamp3 / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    timestamp4 = timestamp - random.randint(60, 3600)
    timestamp_iso4 = datetime.fromtimestamp(timestamp4 / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    timestamp5 = timestamp - random.randint(60, 3600)
    timestamp_iso5 = datetime.fromtimestamp(timestamp5 / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    # Generate random AWS ARNs
    account_id = random.randint(100000000000, 999999999999)
    lambda_arn = generate_random_arn(account_id=account_id)

    j = {
        "message": "mills knee lease",
        "priority": "Critical",
        "status": "naples",
        "time": timestamp,
        "metadata": {
            "version": "1.4.0",
            "product": {
                "name": "australia observe mime",
                "version": "1.4.0",
                "uid": str(uuid.uuid4()),
                "vendor_name": "controversy sally palace",
                "my_dt": timestamp_iso5,
            },
            "profiles": [
                "datetime"
            ],
            "log_name": "ring dancing crown",
            "log_provider": "linear uruguay catalyst",
            "logged_time": 1742372004308,
            "original_time": "flame programs std",
            "tenant_uid": str(uuid.uuid4()),
        },
        "desc": "fiction miss carefully",
        "severity": "Low",
        "duration": 1790528728,
        "category_uid": 2,
        "activity_id": 0,
        "type_uid": 200500,
        "type_name": "Incident Finding: Unknown",
        "category_name": "Findings",
        "class_uid": 2005,
        "class_name": "Incident Finding",
        "timezone_offset": 68,
        "activity_name": "Unknown",
        "confidence": "High",
        "confidence_id": 3,
        "finding_info_list": [
            {
                "title": "mba eventually knit",
                "uid": str(uuid.uuid4()),
                "product_uid": str(uuid.uuid4()),
                "related_events": [
                    {
                        "uid": str(uuid.uuid4()),
                        "severity": "Low",
                        "type_uid": 4071410460,
                        "type_name": "assists described sherman",
                        "kill_chain": [
                            {
                                "phase": "Installation",
                                "phase_id": 5
                            }
                        ],
                        "severity_id": 2
                    },
                    {
                        "type": "dresses potatoes claire",
                        "uid": str(uuid.uuid4()),
                        "severity": "Fatal",
                        "observables": [
                            {
                                "name": "doll ntsc surfing",
                                "type": "IP Address",
                                "type_id": 2
                            }
                        ],
                        "attacks": [
                            {
                                "version": "12.1",
                                "tactics": [
                                    {
                                        "name": "Lateral Movement | The adversary is trying to move through your environment.",
                                        "uid": "TA0008"
                                    }
                                ],
                                "technique": {
                                    "name": "Exfiltration over USB",
                                    "uid": "T1052.001"
                                }
                            },
                            {
                                "version": "12.1",
                                "tactics": [
                                    {
                                        "name": "Execution The adversary is trying to run malicious code.",
                                        "uid": "TA0002"
                                    },
                                    {
                                        "name": "Impact | The adversary is trying to manipulate, interrupt, or destroy your systems and data.",
                                        "uid": "TA0040"
                                    }
                                ],
                                "technique": {
                                    "name": "Compromise Infrastructure",
                                    "uid": "T1584"
                                }
                            },
                            {
                                "semantic": 12,
                                "tactics": [
                                    {
                                        "name": "Execution The adversary is trying to run malicious code.",
                                        "uid": "TA0002"
                                    },
                                    {
                                        "name": "Impact | The adversary is trying to manipulate, interrupt, or destroy your systems and data.",
                                        "uid": "TA0040"
                                    }
                                ],
                                "technique": {
                                    "one": "1",
                                    "two": "2"
                                }
                            }
                        ],
                        "created_time": 1742372004304,
                        "severity_id": 6,
                        "modified_time_dt": timestamp_iso2
                    }
                ],
                "related_events_count": 1,
                "created_time_dt": timestamp_iso3
            },
            {
                "title": "anyway spider requires",
                "uid": str(uuid.uuid4()),
                "analytic": {
                    "name": "copper arthritis space",
                    "type": "Learning (ML/DL)",
                    "version": "1.4.0",
                    "uid": str(uuid.uuid4()),
                    "type_id": 4
                },
                "last_seen_time": 1742372004304,
                "related_analytics": [
                    {
                        "name": "gradually answers writings",
                        "type": "Behavioral",
                        "desc": "rw accompanying gaming",
                        "uid": str(uuid.uuid4()),
                        "type_id": 2,
                        "related_analytics": [
                            {
                                "name": "ping lawn bear",
                                "type": "Fingerprinting",
                                "uid": str(uuid.uuid4()),
                                "type_id": 5
                            },
                            {
                                "name": "statute preserve task",
                                "type": "Indexed Data Match",
                                "version": "1.4.0",
                                "uid": str(uuid.uuid4()),
                                "type_id": 11
                            }
                        ]
                    }
                ],
                "related_events_count": 84,
                "first_seen_time_dt": timestamp_iso4
            }
        ],
        "impact": "Critical",
        "impact_id": 4,
        "impact_score": 56,
        "priority_id": 4,
        "severity_id": 2,
        "src_url": "rp",
        "status_code": "toronto",
        "status_detail": "danger corps find",
        "status_id": 99,
        "verdict": "Managed Externally",
        "verdict_id": 9,
        "time_dt": timestamp_iso
    }

    return j

if __name__ == '__main__':
    for size in [100_000]:
        data_list = [generate_randomized_json(i) for i in range(size)]

    with open("finding.ndjson", "w") as f:
        for record in data_list:
            f.write(json.dumps(record) + "\n")

    input_file = "finding.ndjson"
    output_file = "finding-sorted.ndjson.gz"

    with open(input_file, "rb") as f_in, gzip.open(output_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    awswrangler.s3.upload(output_file, f's3://json-to-parquet-nsmith/findings/{output_file}')


    random.shuffle(data_list)

    with open("finding.ndjson", "w") as f:
        for record in data_list:
            f.write(json.dumps(record) + "\n")

    input_file = "finding.ndjson"
    output_file = "finding-unsorted.ndjson.gz"

    with open(input_file, "rb") as f_in, gzip.open(output_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    awswrangler.s3.upload(output_file, f's3://json-to-parquet-nsmith/findings/{output_file}')
