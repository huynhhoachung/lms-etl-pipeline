# LMS to S3 & RDS ETL

**ETL AWS Lambdas: extract LMS user data into S3 and sync to PostgreSQL RDS with failure alerts**

## Overview
- lms_to_s3_lambda.py: extract data from LMS API, process, and upload CSV to S3.
- s3_to_rds_lambda.py: retrieve CSV from S3, align schema, and upsert into RDS.

## Setup
1. Copy `.env.example` to `.env` and fill values.
2. Install: `pip install -r requirements.txt`.

## Deploy
- Package and deploy each script as AWS Lambda.
- Configure env vars.
- Triggers:
  - lms_to_s3: CloudWatch schedule.
  - s3_to_rds: S3 ObjectCreated on CSV path.

## Notifications
Failures publish to SNS topic; subscribe email/Slack.
