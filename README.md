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
- AWS CLI installed and configured (for local usage)
- Appropriate AWS permissions in both source and destination accounts
- Required Python packages (install using `poetry install`)

## Installation

### Using Poetry (Local Installation)

1. Clone this repository
2. Install dependencies:
   ```bash
   poetry install
   ```

### Using Docker

You can also use the provided Docker image:

```bash
# Build the Docker image
docker build -t aws-copier .

# Run the Docker container
docker run -v $(pwd)/data:/data aws-copier migrate --help
```

## Usage

### Command Line Interface

The tool now supports a command-line interface with the following commands:

#### Migrate Buckets

```bash
# Using Poetry
poetry run python -m src.aws_copier migrate \
  --old-key YOUR_SOURCE_ACCESS_KEY \
  --old-secret YOUR_SOURCE_SECRET_KEY \
  --new-key YOUR_DEST_ACCESS_KEY \
  --new-secret YOUR_DEST_SECRET_KEY \
  [--old-account-id SOURCE_ACCOUNT_ID] \
  [--new-account-id DEST_ACCOUNT_ID] \
  [--source-profile SOURCE_PROFILE] \
  [--dest-profile DEST_PROFILE] \
  [--source-region SOURCE_REGION] \
  [--dest-region DEST_REGION] \
  [--bucket-suffix SUFFIX] \
  [--max-workers NUM_WORKERS] \
  [--blacklist PREFIX1,PREFIX2] \
  [--debug]

# Using Docker
docker run -v $(pwd)/data:/data aws-copier migrate \
  --old-key YOUR_SOURCE_ACCESS_KEY \
  --old-secret YOUR_SOURCE_SECRET_KEY \
  --new-key YOUR_DEST_ACCESS_KEY \
  --new-secret YOUR_DEST_SECRET_KEY \
  [--old-account-id SOURCE_ACCOUNT_ID] \
  [--new-account-id DEST_ACCOUNT_ID] \
  [--source-region SOURCE_REGION] \
  [--dest-region DEST_REGION] \
  [--bucket-suffix SUFFIX] \
  [--max-workers NUM_WORKERS] \
  [--blacklist PREFIX1,PREFIX2] \
  [--debug]
```

#### Check Bucket Status

```bash
# Using Poetry
poetry run python -m src.aws_copier check BUCKET_NAME

# Using Docker
docker run -v $(pwd)/data:/data aws-copier check BUCKET_NAME
```

#### Clean Database

```bash
# Using Poetry
poetry run python -m src.aws_copier clean

# Using Docker
docker run -v $(pwd)/data:/data aws-copier clean
```

### Environment Variables (Legacy Support)

The tool still supports configuration via environment variables through a `.env` file:

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Configure the environment variables in `.env` file

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

## Region Configuration

By default, the script will:
1. Create destination buckets in the same region as their source buckets
2. You can override this behavior by setting:
   - `--source-region`: Forces all source bucket operations to use this region
   - `--dest-region`: Creates all destination buckets in this region

This is useful when you want to:
- Consolidate buckets in a specific region
- Meet data residency requirements
- Optimize for cost or latency by choosing specific regions

## Bucket Policy Migration

Bucket policies are only copied and adapted if both `--old-account-id` and `--new-account-id` parameters are provided. If either is missing, the policy copy step is skipped and only the bucket contents are synchronized. This is useful when:
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

When using Docker, the database is stored in the `/data` volume, which can be mapped to a local directory for persistence.

## Troubleshooting

- Check logs for detailed error messages
- Verify AWS credentials and permissions
- Ensure AWS CLI is properly configured if using profiles
- Review the database for bucket states using the `check` command