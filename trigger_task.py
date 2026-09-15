#!/usr/bin/env python3
"""
Trigger an ECS task for virtualzarr-gen from your local machine.

Usage:
    python trigger_task.py --collection OSTIA-UKMO-L4-GLOB-REP-v2.0 --stage sit
    python trigger_task.py --collection MUR25-JPL-L4-GLOB-v04.2 --stage sit --engine icechunk
    python trigger_task.py --collection MUR25-JPL-L4-GLOB-v04.2 --stage sit --start-date 2020-01-01 --end-date 2023-01-01
"""

import argparse
import sys

import boto3


STAGES = {
    "sit": {
        "output_bucket": "podaac-sit-services-cloud-optimizer",
        "staging_bucket": "simon-vds-staging",
        "ssm_edl_username": "generate-edl-username",
        "ssm_edl_password": "generate-edl-password",
    },
    "uat": {
        "output_bucket": "podaac-uat-services-cloud-optimizer",
        "staging_bucket": "simon-vds-staging",
        "ssm_edl_username": "generate-edl-username",
        "ssm_edl_password": "generate-edl-password",
    },
    "ops": {
        "output_bucket": "podaac-ops-services-cloud-optimizer",
        "staging_bucket": "",
        "ssm_edl_username": "generate-edl-username",
        "ssm_edl_password": "generate-edl-password",
    },
}

RESOURCE_PREFIX = "service-virtualzarr-gen"


def run_task(args):
    stage_config = STAGES[args.stage]
    cluster = f"{RESOURCE_PREFIX}-{args.stage}-cluster"
    task_definition = f"{RESOURCE_PREFIX}-{args.stage}-app-task"

    env_overrides = [
        {"name": "COLLECTION", "value": args.collection},
        {"name": "LOADABLE_VARS", "value": args.loadable_vars},
        {"name": "OUTPUT_BUCKET", "value": stage_config["output_bucket"]},
        {"name": "STAGING_BUCKET", "value": stage_config["staging_bucket"]},
        {"name": "SSM_EDL_USERNAME", "value": stage_config["ssm_edl_username"]},
        {"name": "SSM_EDL_PASSWORD", "value": stage_config["ssm_edl_password"]},
        {"name": "START_DATE", "value": args.start_date or ""},
        {"name": "END_DATE", "value": args.end_date or ""},
        {"name": "CPU_COUNT", "value": str(args.cpu_count)},
        {"name": "MEMORY_LIMIT", "value": args.memory_limit},
        {"name": "BATCH_SIZE", "value": str(args.batch_size)},
        {"name": "ENGINE", "value": args.engine},
    ]

    ecs = boto3.client("ecs", region_name="us-west-2")
    ec2 = boto3.client("ec2", region_name="us-west-2")

    vpcs = ec2.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": ["Application VPC"]}])
    if not vpcs["Vpcs"]:
        print("ERROR: Could not find Application VPC", file=sys.stderr)
        sys.exit(1)
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    subnets = ec2.describe_subnets(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "tag:Name", "Values": ["Private application*"]},
        ]
    )
    subnet_ids = [s["SubnetId"] for s in subnets["Subnets"]]

    sgs = ec2.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["default"]},
        ]
    )
    sg_ids = [sg["GroupId"] for sg in sgs["SecurityGroups"]]

    print(f"Cluster:         {cluster}")
    print(f"Task definition: {task_definition}")
    print(f"Collection:      {args.collection}")
    print(f"Engine:          {args.engine}")
    print(f"Stage:           {args.stage}")
    print()

    response = ecs.run_task(
        cluster=cluster,
        taskDefinition=task_definition,
        count=1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnet_ids,
                "securityGroups": sg_ids,
                "assignPublicIp": "DISABLED",
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": "cloud-optimization-generation",
                    "environment": env_overrides,
                }
            ]
        },
        capacityProviderStrategy=[
            {
                "capacityProvider": f"{RESOURCE_PREFIX}-{args.stage}-ecs-capacity-provider",
                "weight": 100,
                "base": 1,
            }
        ],
    )

    failures = response.get("failures", [])
    if failures:
        print("Task failed to launch:")
        for f in failures:
            print(f"  {f.get('reason', 'unknown')}: {f.get('detail', '')}")
        sys.exit(1)

    task_arn = response["tasks"][0]["taskArn"]
    task_id = task_arn.split("/")[-1]
    print(f"Task launched: {task_id}")
    print(f"Task ARN:      {task_arn}")


def main():
    parser = argparse.ArgumentParser(
        description="Trigger virtualzarr-gen ECS task",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--collection", required=True, help="Earthdata collection short name")
    parser.add_argument("--stage", required=True, choices=["sit", "uat", "ops"], help="Deployment stage")
    parser.add_argument("--engine", default="icechunk", choices=["kerchunk", "icechunk"], help="Processing engine (default: icechunk)")
    parser.add_argument("--loadable-vars", default="latitude,longitude,time", help="Comma-separated coordinate vars")
    parser.add_argument("--start-date", default=None, help="Start date (e.g., 2022-01-01)")
    parser.add_argument("--end-date", default=None, help="End date (e.g., 2025-01-01)")
    parser.add_argument("--cpu-count", type=int, default=96, help="Number of Dask workers")
    parser.add_argument("--memory-limit", default="6GB", help="Memory limit per worker")
    parser.add_argument("--batch-size", type=int, default=960, help="Batch size")

    args = parser.parse_args()
    run_task(args)


if __name__ == "__main__":
    main()
