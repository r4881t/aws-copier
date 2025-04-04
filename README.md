# AWS S3 Bucket Copier

A tool to copy S3 buckets from one AWS account to another, with optional bucket policy migration.

## Features

- Copies buckets from source to destination AWS account
- Supports different source and destination regions
- Optionally preserves and adapts bucket policies (when account IDs are provided)
- Uses AWS CLI for efficient data synchronization
- Maintains state in SQLite database to support resuming interrupted operations
- Processes multiple buckets in parallel
- Supports both AWS access keys and AWS CLI profiles for authentication

## Prerequisites

- Python 3.7+
- AWS CLI installed and configured
- Appropriate AWS permissions in both source and destination accounts
- Required Python packages (install using `poetry install`)

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   poetry install
   ```
3. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
4. Configure the environment variables in `.env` file:
   - Required variables:
     - OLD_AWS_ACCESS_KEY_ID and OLD_AWS_SECRET_ACCESS_KEY for source account
     - NEW_AWS_ACCESS_KEY_ID and NEW_AWS_SECRET_ACCESS_KEY for destination account
   - Optional variables:
     - SOURCE_BUCKET_REGION to specify the source bucket region
     - DEST_BUCKET_REGION to specify the destination bucket region
     - OLD_AWS_ACCOUNT_ID and NEW_AWS_ACCOUNT_ID (only needed if you want to copy bucket policies)
     - AWS_PROFILE_SOURCE and AWS_PROFILE_DEST for using AWS CLI profiles

## Region Configuration

By default, the script will:
1. Create destination buckets in the same region as their source buckets
2. You can override this behavior by setting:
   - SOURCE_BUCKET_REGION: Forces all source bucket operations to use this region
   - DEST_BUCKET_REGION: Creates all destination buckets in this region

This is useful when you want to:
- Consolidate buckets in a specific region
- Meet data residency requirements
- Optimize for cost or latency by choosing specific regions

## Required AWS Permissions

### Source Account
- s3:ListBuckets
- s3:GetBucketLocation
- s3:GetBucketPolicy (only if copying policies)
- s3:ListBucket
- s3:GetObject

### Destination Account
- s3:CreateBucket
- s3:PutBucketPolicy (only if copying policies)
- s3:PutObject

## Usage

Run the script:
```bash
poetry run python src/aws_copier.py
```

The script will:
1. Discover all buckets in the source account
2. Create corresponding buckets in the destination account (with "_s" suffix)
3. Copy and adapt bucket policies (if AWS account IDs are provided)
4. Sync bucket contents using AWS CLI

Progress is tracked in `s3_migration_state.db` and the script can be safely interrupted and resumed.

## Bucket Policy Migration

Bucket policies are only copied and adapted if both OLD_AWS_ACCOUNT_ID and NEW_AWS_ACCOUNT_ID environment variables are provided. If either is missing, the policy copy step is skipped and only the bucket contents are synchronized. This is useful when:
- You don't need to preserve bucket policies
- You want to set up new policies manually
- The source buckets don't have any policies

## State Management

The script maintains state in an SQLite database (`s3_migration_state.db`). Each bucket goes through these states:
- discovered: Initial state when bucket is found
- created: Destination bucket has been created
- policy_copied: Bucket policy has been copied and adapted (or skipped if account IDs not provided)
- sync_started: Data sync has begun
- sync_completed: All data has been copied
- failed: An error occurred (check logs for details)

## Troubleshooting

- Check logs for detailed error messages
- Verify AWS credentials and permissions
- Ensure AWS CLI is properly configured if using profiles
- Review `s3_migration_state.db` for bucket states