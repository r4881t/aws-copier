import boto3
import os
import sqlite3
import json
import logging
import subprocess
import sys
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional, Any, Tuple, List
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from botocore.config import Config
import time
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# --- Configuration ---
DEST_BUCKET_SUFFIX = "-s0"
MAX_WORKERS = 10  # Number of buckets to process in parallel
DB_NAME = os.environ.get("DB_NAME", "s3_migration_state.db")
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
DEBUG_MODE = os.environ.get('DEBUG_MODE', 'false').lower() in ('true', '1', 'yes')

# Memory management configuration
MAX_CHUNK_SIZE = 256 * 1024 * 1024  # 256MB chunks instead of 1GB
MAX_MEMORY_PER_WORKER = 512 * 1024 * 1024  # 512MB per worker
MEMORY_BUFFER_FACTOR = 0.8  # Use 80% of available memory

# Status constants
STATUS_DISCOVERED = 'discovered'
STATUS_CREATED = 'created'
STATUS_POLICY_COPIED = 'policy_copied'
STATUS_SYNC_STARTED = 'sync_started'
STATUS_SYNC_COMPLETED = 'sync_completed'
STATUS_FAILED = 'failed'
# Set logging level based on DEBUG_MODE
logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

if DEBUG_MODE:
    logger.info("Debug mode enabled - detailed logging will be shown")
logger = logging.getLogger(__name__)

# Thread-local storage for SQLite connections
thread_local = threading.local()

# --- Database Functions ---

def get_db_connection() -> sqlite3.Connection:
    """Get a thread-local database connection."""
    if not hasattr(thread_local, "db_connection"):
        thread_local.db_connection = sqlite3.connect(DB_NAME)
        thread_local.db_connection.execute("PRAGMA journal_mode=WAL")  # Use WAL mode for better concurrency
    return thread_local.db_connection

def init_db(db_name: str) -> bool:
    """Initializes the SQLite database and table.
    Returns True if the database was created, False if it already existed.
    """
    db_existed = os.path.exists(db_name)
    conn = sqlite3.connect(db_name)
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS buckets (
                source_bucket_name TEXT PRIMARY KEY,
                dest_bucket_name TEXT,
                region TEXT,
                status TEXT NOT NULL,
                last_sync_output TEXT,
                error_message TEXT
            )
        ''')
        conn.commit()
        logger.info(f"Database '{db_name}' {'accessed' if db_existed else 'created'}.")
        return not db_existed  # Return True if it's a new database
    finally:
        conn.close()

def add_bucket_to_db(source_bucket: str, dest_bucket: str):
    """Adds a newly discovered bucket to the DB if it doesn't exist."""
    conn = get_db_connection()
    try:
        # Check if the bucket already exists in the database
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM buckets WHERE source_bucket_name = ?", (source_bucket,))
        result = cursor.fetchone()
        
        if result:
            # If bucket exists but is in sync_started status (might have been interrupted),
            # reset it to discovered to ensure proper sequence
            if result[0] == STATUS_SYNC_STARTED:
                logger.info(f"Resetting bucket {source_bucket} from sync_started to discovered state")
                conn.execute(
                    "UPDATE buckets SET status = ? WHERE source_bucket_name = ?",
                    (STATUS_DISCOVERED, source_bucket)
                )
        else:
            # Insert new bucket with discovered status
            conn.execute(
                "INSERT INTO buckets (source_bucket_name, dest_bucket_name, status) VALUES (?, ?, ?)",
                (source_bucket, dest_bucket, STATUS_DISCOVERED)
            )
            logger.info(f"Added bucket {source_bucket} with initial status: {STATUS_DISCOVERED}")
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"DB Error adding {source_bucket}: {e}")

def update_bucket_status(source_bucket: str, status: str, region: Optional[str] = None, error: Optional[str] = None, sync_output: Optional[str] = None):
    """Updates the status and optionally region/error/sync_output of a bucket."""
    conn = get_db_connection()
    try:
        if region:
            conn.execute("UPDATE buckets SET status = ?, region = ?, error_message = ?, last_sync_output = ? WHERE source_bucket_name = ?",
                          (status, region, error, sync_output, source_bucket))
        else:
            conn.execute("UPDATE buckets SET status = ?, error_message = ?, last_sync_output = ? WHERE source_bucket_name = ?",
                          (status, error, sync_output, source_bucket))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"DB Error updating status for {source_bucket}: {e}")


def get_buckets_to_process() -> List[Tuple[str, str, Optional[str], str]]:
    """Gets buckets that are not yet fully completed."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Get buckets that need processing, but ignore ones that have completed sync
        cursor.execute(
            "SELECT source_bucket_name, dest_bucket_name, region, status FROM buckets WHERE status != ?",
            (STATUS_SYNC_COMPLETED,)
        )
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"DB Error fetching buckets to process: {e}")
        return []

def reset_incomplete_tasks():
    """Reset any buckets that might be in an incomplete state to ensure proper sequence."""
    conn = get_db_connection()
    try:
        # Get all buckets in sync_started state - these might have been interrupted
        cursor = conn.cursor()
        cursor.execute("SELECT source_bucket_name FROM buckets WHERE status = ?", (STATUS_SYNC_STARTED,))
        sync_started_buckets = [row[0] for row in cursor.fetchall()]
        
        if sync_started_buckets:
            logger.info(f"Found {len(sync_started_buckets)} buckets in sync_started state that may have been interrupted.")
            for bucket in sync_started_buckets:
                logger.info(f"Resetting bucket {bucket} to discovered state to ensure proper migration sequence.")
                conn.execute("UPDATE buckets SET status = ? WHERE source_bucket_name = ?", 
                            (STATUS_DISCOVERED, bucket))
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"DB Error resetting incomplete tasks: {e}")

# --- AWS Helper Functions ---

def get_boto_client(aws_access_key_id: str, aws_secret_access_key: str, service: str = 's3', region_name: Optional[str] = None) -> Optional[Any]:
    """Creates a Boto3 client for the specified service and credentials."""
    try:
        # Increase connect timeout and read timeout for potentially long operations
        config = Config(
            connect_timeout=20,
            read_timeout=60,
            retries={'max_attempts': 5}
        )
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name # Can be None initially
        )
        return session.client(service, config=config)
    except (NoCredentialsError, PartialCredentialsError):
        logger.error(f"AWS credentials not found or incomplete for Boto3 client creation.")
        return None
    except Exception as e:
        logger.error(f"Error creating Boto3 client for {service}: {e}")
        return None

def get_bucket_location(s3_client: Any, bucket_name: str) -> Optional[str]:
    """Gets the region of a bucket."""
    try:
        response = s3_client.get_bucket_location(Bucket=bucket_name)
        region = response.get('LocationConstraint')
        # Buckets in us-east-1 return None or empty string for LocationConstraint
        return region if region else 'us-east-1'
    except ClientError as e:
        # Handle potential access denied or NoSuchBucket errors gracefully
        if e.response['Error']['Code'] == 'NoSuchBucket':
             logger.error(f"Source bucket {bucket_name} not found.")
        elif e.response['Error']['Code'] == 'AccessDenied':
             logger.error(f"Access denied getting location for source bucket {bucket_name}.")
        else:
             logger.error(f"Error getting location for {bucket_name}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting location for {bucket_name}: {e}")
        return None


def adapt_bucket_policy(policy_str: str, old_account_id: str, new_account_id: str) -> str:
    """
    Adapts the bucket policy by replacing account IDs.
    WARNING: This is a basic replacement and might not cover all cases,
             especially complex ARNs or conditions. Review adapted policies carefully!
    """
    logger.debug(f"Adapting policy for account {old_account_id} -> {new_account_id}")
    # Replace Account ID in Principal ARNs, Resource ARNs, etc.
    adapted_policy = policy_str.replace(old_account_id, new_account_id)

    # Potentially add more sophisticated replacements here if needed
    # e.g., using regex for specific ARN patterns

    if adapted_policy != policy_str:
        logger.info("Policy adapted with new account ID.")
    else:
         logger.info("No account ID replacements made in the policy.")
    return adapted_policy

def get_optimal_chunk_size(file_size: int) -> int:
    """Calculate optimal chunk size based on file size and memory constraints."""
    # For small files (< 1GB), use smaller chunks
    if file_size < 1024 * 1024 * 1024:
        return min(file_size, MAX_CHUNK_SIZE)
    
    # For large files, calculate chunks to fit in memory
    chunks_needed = (file_size + MAX_MEMORY_PER_WORKER - 1) // MAX_MEMORY_PER_WORKER
    optimal_chunk_size = (file_size + chunks_needed - 1) // chunks_needed
    
    # Never exceed MAX_CHUNK_SIZE
    return min(optimal_chunk_size, MAX_CHUNK_SIZE)

def sync_data(source_bucket: str, dest_bucket: str, source_profile: Optional[str], dest_profile: Optional[str], source_credentials: Optional[Dict] = None, dest_credentials: Optional[Dict] = None) -> Tuple[bool, str]:
    """
    Uses boto3 to directly copy objects between buckets.
    Returns a tuple of (success, output_message)
    """
    output_messages = []
    logger.info(f"[{source_bucket}] Starting sync operation to {dest_bucket}")
    start_time = time.time()
    last_progress_time = start_time
    try:
        # Create source and destination S3 clients based on provided credentials or profiles
        if source_credentials:
            source_session = boto3.Session(
                aws_access_key_id=source_credentials['aws_access_key_id'],
                aws_secret_access_key=source_credentials['aws_secret_access_key']
            )
        else:
            source_session = boto3.Session(profile_name=source_profile) if source_profile else boto3.Session()
            
        if dest_credentials:
            dest_session = boto3.Session(
                aws_access_key_id=dest_credentials['aws_access_key_id'],
                aws_secret_access_key=dest_credentials['aws_secret_access_key']
            )
        else:
            dest_session = boto3.Session(profile_name=dest_profile) if dest_profile else boto3.Session()
        
        s3_source = source_session.client('s3')
        s3_dest = dest_session.client('s3')
        
        # Use paginator to handle large buckets
        paginator = s3_source.get_paginator('list_objects_v2')
        total_objects = 0
        transferred_objects = 0
        
        # Iterate through all objects in source bucket
        page_count = 0
        for page in paginator.paginate(Bucket=source_bucket):
            page_count += 1
            if 'Contents' not in page:
                logger.debug(f"[{source_bucket}] Page {page_count} has no contents")
                continue
            
            logger.debug(f"[{source_bucket}] Processing page {page_count} with {len(page['Contents'])} objects")
                
            for obj in page['Contents']:
                total_objects += 1
                source_key = obj['Key']
                
                try:
                    # Check if object needs to be copied by comparing ETags
                    try:
                        dest_obj = s3_dest.head_object(Bucket=dest_bucket, Key=source_key)
                        if dest_obj['ETag'] == obj['ETag']:
                            logger.debug(f"[{source_bucket}] Object {source_key} already exists with same ETag, skipping.")
                            continue
                    except ClientError as e:
                        if e.response['Error']['Code'] != '404':
                            raise
                    
                    # Copy object
                    copy_source = {
                        'Bucket': source_bucket,
                        'Key': source_key
                    }
                    
                    # Calculate optimal chunk size based on file size
                    chunk_size = get_optimal_chunk_size(obj['Size'])
                    
                    # For files requiring multipart upload
                    if obj['Size'] > 5 * 1024 * 1024 * 1024 or chunk_size < obj['Size']:  # 5GB or needs chunking
                        size_str = f"{obj['Size']/(1024*1024):.2f}MB" if obj['Size'] < 1024*1024*1024 else f"{obj['Size']/(1024*1024*1024):.2f}GB"
                        msg = f"Starting multipart copy for object: {source_key} ({size_str}) with chunk size: {chunk_size/(1024*1024):.2f}MB"
                        # Use INFO level for large file transfers so they're visible without debug mode
                        logger.info(f"[{source_bucket}] {msg}")
                        output_messages.append(msg)
                        # Get source object metadata
                        response = s3_source.head_object(Bucket=source_bucket, Key=source_key)
                        content_type = response.get('ContentType', 'application/octet-stream')
                        
                        # Initiate multipart upload
                        mpu = s3_dest.create_multipart_upload(
                            Bucket=dest_bucket,
                            Key=source_key,
                            ContentType=content_type
                        )
                        # Copy parts
                        parts = []
                        total_parts = (obj['Size'] + chunk_size - 1) // chunk_size
                        
                        multipart_start_time = time.time()
                        for i in range(total_parts):
                            start = i * chunk_size
                            end = min(start + chunk_size - 1, obj['Size'] - 1)
                            
                            part_size_mb = (end - start + 1) / (1024 * 1024)
                            logger.debug(f"[{source_bucket}] Uploading part {i+1}/{total_parts} for {source_key} (part size: {part_size_mb:.2f}MB)")
                            
                            part_start_time = time.time()
                            response = s3_dest.upload_part_copy(
                                Bucket=dest_bucket,
                                Key=source_key,
                                PartNumber=i + 1,
                                UploadId=mpu['UploadId'],
                                CopySource=copy_source,
                                CopySourceRange=f'bytes={start}-{end}'
                            )
                            part_duration = time.time() - part_start_time
                            logger.debug(f"[{source_bucket}] Completed part {i+1}/{total_parts} for {source_key} in {part_duration:.2f} seconds")
                            
                            parts.append({
                                'PartNumber': i + 1,
                                'ETag': response['CopyPartResult']['ETag']
                            })
                            
                            # Release memory after each part
                            del response
                        
                        # Complete multipart upload
                        s3_dest.complete_multipart_upload(
                            Bucket=dest_bucket,
                            Key=source_key,
                            UploadId=mpu['UploadId'],
                            MultipartUpload={'Parts': parts}
                        )
                        
                        multipart_duration = time.time() - multipart_start_time
                        size_str = f"{obj['Size']/(1024*1024):.2f}MB" if obj['Size'] < 1024*1024*1024 else f"{obj['Size']/(1024*1024*1024):.2f}GB"
                        # Use INFO level for completion of large file transfers
                        logger.info(f"[{source_bucket}] Completed multipart upload for {source_key} ({size_str}) in {multipart_duration:.2f} seconds")
                    else:
                        size_str = f"{obj['Size']/1024:.2f}KB" if obj['Size'] < 1024*1024 else f"{obj['Size']/(1024*1024):.2f}MB"
                        logger.debug(f"[{source_bucket}] Copying object {source_key} (size: {size_str})")
                        start_time = time.time()
                        s3_dest.copy(copy_source, dest_bucket, source_key)
                        duration = time.time() - start_time
                        logger.debug(f"[{source_bucket}] Copied {source_key} in {duration:.2f} seconds")
                    
                    transferred_objects += 1
                    # Log progress more frequently in debug mode
                    log_frequency = 10 if DEBUG_MODE else 100
                    if transferred_objects % log_frequency == 0:
                        msg = f"Transferred {transferred_objects}/{total_objects} objects..."
                        # Use INFO level for regular progress updates so they're visible without debug mode
                        logger.info(f"[{source_bucket}] {msg}")
                        output_messages.append(msg)
                        
                        # Provide a periodic summary at INFO level regardless of log_frequency
                        current_time = time.time()
                        # Log a summary every 60 seconds
                        if current_time - last_progress_time > 60:
                            elapsed = current_time - start_time
                            objects_per_second = transferred_objects / elapsed if elapsed > 0 else 0
                            percent_complete = (transferred_objects / total_objects * 100) if total_objects > 0 else 0
                            logger.info(f"[{source_bucket}] Progress summary: {transferred_objects}/{total_objects} objects ({percent_complete:.1f}%) transferred in {elapsed:.1f} seconds ({objects_per_second:.2f} objects/sec)")
                            last_progress_time = current_time
                        
                except Exception as e:
                    error_msg = f"Error copying object {source_key}: {str(e)}"
                    logger.error(f"[{source_bucket}] {error_msg}")
                    output_messages.append(error_msg)
                    continue
                
                # Force garbage collection after large transfers
                if obj['Size'] > MAX_MEMORY_PER_WORKER:
                    import gc
                    gc.collect()
        success = True
        final_message = f"Completed transfer of {transferred_objects}/{total_objects} objects."
        logger.info(f"[{source_bucket}] {final_message}")
        output_messages.append(final_message)
        
        
    except Exception as e:
        success = False
        error_message = f"Error during sync operation: {str(e)}"
        logger.error(f"[{source_bucket}] {error_message}")
        output_messages.append(error_message)
    
    return success, "\n".join(output_messages)

# --- Main Processing Function ---

def process_bucket(
    source_bucket_name: str,
    dest_bucket_name: str,
    initial_region: Optional[str],
    initial_status: str,
    s3_client_old: Any,
    s3_client_new: Any,
    old_account_id: str,
    new_account_id: str,
    source_profile: Optional[str],
    dest_profile: Optional[str],
    dest_region: Optional[str] = None,  # New parameter for destination region
) -> Tuple[str, str]:
    """Processes a single bucket through the migration steps."""
    current_status = initial_status
    current_region = initial_region
    logger.info(f"Processing {source_bucket_name} (Dest: {dest_bucket_name}) - Initial status: {current_status}")

    try:
        # --- Step 1: Get Region (if needed) & Create Bucket ---
        if current_status == STATUS_DISCOVERED:
            logger.info(f"[{source_bucket_name}] Getting source region...")
            current_region = get_bucket_location(s3_client_old, source_bucket_name)
            if not current_region:
                update_bucket_status(source_bucket_name, STATUS_FAILED, error="Failed to get source bucket location")
                return source_bucket_name, STATUS_FAILED

            # Use destination region if specified, otherwise use source region
            target_region = dest_region or current_region
            update_bucket_status(source_bucket_name, STATUS_DISCOVERED, region=current_region)
            logger.info(f"[{source_bucket_name}] Source Region: {current_region}, Target Region: {target_region}. Creating destination bucket {dest_bucket_name}...")

            try:
                # Get a new session for the destination region
                new_session = boto3.Session(
                    aws_access_key_id=os.environ.get('NEW_AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('NEW_AWS_SECRET_ACCESS_KEY'),
                    region_name=target_region
                )
                s3_client_new_regional = new_session.client('s3')

                create_kwargs = {'Bucket': dest_bucket_name}
                if target_region != 'us-east-1':
                    create_kwargs['CreateBucketConfiguration'] = {'LocationConstraint': target_region}

                s3_client_new_regional.create_bucket(**create_kwargs)
                logger.info(f"[{source_bucket_name}] Created destination bucket {dest_bucket_name} in {target_region}")
                current_status = STATUS_CREATED
                update_bucket_status(source_bucket_name, current_status, region=current_region)

            except ClientError as e:
                if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                    logger.warning(f"[{source_bucket_name}] Destination bucket {dest_bucket_name} already exists and is owned by you. Proceeding.")
                    current_status = STATUS_CREATED
                    update_bucket_status(source_bucket_name, current_status, region=current_region)
                elif e.response['Error']['Code'] == 'BucketAlreadyExists':
                    logger.error(f"[{source_bucket_name}] Destination bucket name {dest_bucket_name} is already taken globally.")
                    update_bucket_status(source_bucket_name, STATUS_FAILED, region=current_region, error="Destination bucket name already exists globally")
                    return source_bucket_name, STATUS_FAILED
                else:
                    logger.error(f"[{source_bucket_name}] Error creating bucket {dest_bucket_name}: {e}")
                    update_bucket_status(source_bucket_name, STATUS_FAILED, region=current_region, error=f"Create bucket error: {e}")
                    return source_bucket_name, STATUS_FAILED

        # --- Step 2: Copy Policy (if account IDs are provided) ---
        if current_status == STATUS_CREATED:
            # Skip policy copy if account IDs are not provided
            if not old_account_id or not new_account_id:
                logger.info(f"[{source_bucket_name}] Skipping policy copy as AWS account IDs are not provided.")
                current_status = STATUS_POLICY_COPIED
                update_bucket_status(source_bucket_name, current_status)
            else:
                # Recreate client with specific region if not us-east-1
                s3_client_new_regional = get_boto_client(
                    os.environ.get('NEW_AWS_ACCESS_KEY_ID'),
                    os.environ.get('NEW_AWS_SECRET_ACCESS_KEY'),
                    region_name=current_region
                )
                if not s3_client_new_regional:
                     raise Exception("Failed to create regional client for destination policy step.")

                logger.info(f"[{source_bucket_name}] Getting policy...")
                try:
                    policy_response = s3_client_old.get_bucket_policy(Bucket=source_bucket_name)
                    original_policy = policy_response['Policy']
                    logger.info(f"[{source_bucket_name}] Found policy. Adapting...")

                    adapted_policy = adapt_bucket_policy(original_policy, old_account_id, new_account_id)

                    logger.info(f"[{source_bucket_name}] Applying adapted policy to {dest_bucket_name}...")
                    s3_client_new_regional.put_bucket_policy(Bucket=dest_bucket_name, Policy=adapted_policy)
                    logger.info(f"[{source_bucket_name}] Policy applied.")
                    current_status = STATUS_POLICY_COPIED
                    update_bucket_status(source_bucket_name, current_status)

                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
                        logger.info(f"[{source_bucket_name}] No policy found on source bucket. Skipping policy copy.")
                        current_status = STATUS_POLICY_COPIED
                        update_bucket_status(source_bucket_name, current_status)
                    else:
                        logger.error(f"[{source_bucket_name}] Error getting/putting policy for {source_bucket_name}/{dest_bucket_name}: {e}")
                        update_bucket_status(source_bucket_name, STATUS_FAILED, error=f"Policy error: {e}")
                        return source_bucket_name, STATUS_FAILED

        # --- Step 3: Sync Data ---
        # Allow retrying sync if it failed previously or was interrupted
        if current_status in [STATUS_POLICY_COPIED, STATUS_SYNC_STARTED, STATUS_FAILED]:
             logger.info(f"[{source_bucket_name}] Starting data sync to {dest_bucket_name}...")
             update_bucket_status(source_bucket_name, STATUS_SYNC_STARTED, error=None, sync_output=None)

             time.sleep(2)

             # Create credentials dictionaries from environment variables
             source_credentials = {
                 'aws_access_key_id': os.environ.get('OLD_AWS_ACCESS_KEY_ID'),
                 'aws_secret_access_key': os.environ.get('OLD_AWS_SECRET_ACCESS_KEY')
             }
             dest_credentials = {
                 'aws_access_key_id': os.environ.get('NEW_AWS_ACCESS_KEY_ID'),
                 'aws_secret_access_key': os.environ.get('NEW_AWS_SECRET_ACCESS_KEY')
             }

             success, sync_output = sync_data(
                 source_bucket_name, 
                 dest_bucket_name, 
                 source_profile, 
                 dest_profile,
                 source_credentials=source_credentials,
                 dest_credentials=dest_credentials
             )

             if success:
                 logger.info(f"[{source_bucket_name}] Sync completed successfully. {sync_output.splitlines()[-1] if sync_output else ''}")
                 current_status = STATUS_SYNC_COMPLETED
                 update_bucket_status(source_bucket_name, current_status, sync_output=sync_output)
             else:
                 logger.error(f"[{source_bucket_name}] Sync failed.")
                 current_status = STATUS_FAILED
                 update_bucket_status(source_bucket_name, current_status, error="Sync command failed.", sync_output=sync_output)

        return source_bucket_name, current_status

    except Exception as e:
        logger.exception(f"[{source_bucket_name}] Unhandled exception during processing:")
        update_bucket_status(source_bucket_name, STATUS_FAILED, error=f"Unhandled exception: {str(e)}", region=current_region)
        return source_bucket_name, STATUS_FAILED

# --- Main Execution ---

def cleanup_db():
    """Clean up the SQLite database file and its associated WAL files."""
    try:
        # Close all connections first
        for thread in threading.enumerate():
            if hasattr(thread_local, "db_connection"):
                thread_local.db_connection.close()
                delattr(thread_local, "db_connection")
        
        # Remove the database files
        if os.path.exists(DB_NAME):
            os.remove(DB_NAME)
        if os.path.exists(DB_NAME + "-wal"):
            os.remove(DB_NAME + "-wal")
        if os.path.exists(DB_NAME + "-shm"):
            os.remove(DB_NAME + "-shm")
        logger.info("Database files cleaned up successfully.")
    except Exception as e:
        logger.error(f"Error cleaning up database files: {e}")

def validate_credentials(s3_client: Any, account_description: str) -> bool:
    """Validates that the provided credentials work by testing a simple S3 operation."""
    try:
        s3_client.list_buckets()
        return True
    except (NoCredentialsError, ClientError) as e:
        logger.error(f"Failed to validate {account_description} credentials: {str(e)}")
        return False

def get_valid_bucket_name(source_bucket: str, suffix: str) -> str:
    """
    Create a valid S3 bucket name with the specified suffix.
    S3 bucket names must:
    - Contain only lowercase letters, numbers, dots (.), and hyphens (-)
    - Begin and end with a letter or number
    - Be between 3 and 63 characters long
    """
    import re
    
    # Ensure source bucket is lowercase
    source_bucket = source_bucket.lower()
    
    # Ensure suffix starts with a valid separator
    if suffix and not suffix.startswith('-') and not suffix.startswith('_'):
        suffix = '-' + suffix
    
    # Create the destination bucket name
    dest_name = source_bucket + suffix
    
    # S3 bucket names are limited to 63 characters
    if len(dest_name) > 63:
        # If too long, truncate the source name to fit the suffix
        max_source_len = 63 - len(suffix)
        dest_name = source_bucket[:max_source_len] + suffix
    
    # S3 bucket names can only contain lowercase letters, numbers, dots, and hyphens
    # and must begin and end with a letter or number
    if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', dest_name):
        # Clean up the name to make it valid
        # Replace invalid characters with hyphens
        dest_name = re.sub(r'[^a-z0-9.-]', '-', dest_name.lower())
        # Ensure it starts and ends with a valid character
        if not dest_name[0].isalnum():
            dest_name = 's' + dest_name[1:]
        if not dest_name[-1].isalnum():
            dest_name = dest_name[:-1] + 's'
    
    # Log the transformation for debugging
    if dest_name != source_bucket + suffix:
        logger.info(f"Transformed bucket name: {source_bucket + suffix} -> {dest_name}")
    
    return dest_name

def check_bucket_status(bucket_name: str):
    """Display detailed status information for a specific bucket."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT source_bucket_name, dest_bucket_name, status, region, error_message, last_sync_output FROM buckets WHERE source_bucket_name = ?",
            (bucket_name,)
        )
        bucket_info = cursor.fetchone()
        
        if not bucket_info:
            logger.info(f"Bucket '{bucket_name}' not found in the database.")
            return
            
        source, dest, status, region, error, sync_output = bucket_info
        
        logger.info(f"===== BUCKET STATUS: {source} =====")
        logger.info(f"Destination bucket: {dest}")
        logger.info(f"Current status: {status}")
        if region:
            logger.info(f"Region: {region}")
        
        if status == STATUS_FAILED:
            logger.info("--- ERROR DETAILS ---")
            if error:
                logger.info(f"Error message: {error}")
            if sync_output:
                logger.info("--- SYNC OUTPUT ---")
                logger.info(sync_output)
        elif sync_output:
            logger.info("--- SYNC OUTPUT ---")
            logger.info(sync_output)
            
        logger.info("=" * 50)
            
    except sqlite3.Error as e:
        logger.error(f"DB Error fetching bucket details: {e}")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='AWS S3 Bucket Migration Tool',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Migrate command - main functionality
    migrate_parser = subparsers.add_parser('migrate', help='Migrate S3 buckets from source to destination account')
    
    # Required arguments group
    required_group = migrate_parser.add_argument_group('required arguments')
    required_group.add_argument('--old-key', dest='old_key', help='AWS Access Key ID for source account', required=True)
    required_group.add_argument('--old-secret', dest='old_secret', help='AWS Secret Access Key for source account', required=True)
    required_group.add_argument('--new-key', dest='new_key', help='AWS Access Key ID for destination account', required=True)
    required_group.add_argument('--new-secret', dest='new_secret', help='AWS Secret Access Key for destination account', required=True)
    
    # Optional arguments
    migrate_parser.add_argument('--old-account-id', dest='old_account_id', help='AWS Account ID for source account (for policy migration)')
    migrate_parser.add_argument('--new-account-id', dest='new_account_id', help='AWS Account ID for destination account (for policy migration)')
    migrate_parser.add_argument('--source-profile', dest='source_profile', help='AWS CLI profile for source account')
    migrate_parser.add_argument('--dest-profile', dest='dest_profile', help='AWS CLI profile for destination account')
    migrate_parser.add_argument('--source-region', dest='source_region', help='AWS region for source buckets')
    migrate_parser.add_argument('--dest-region', dest='dest_region', help='AWS region for destination buckets')
    migrate_parser.add_argument('--bucket-suffix', dest='bucket_suffix', default=DEST_BUCKET_SUFFIX,
                               help='Suffix to append to destination bucket names')
    migrate_parser.add_argument('--max-workers', dest='max_workers', type=int, default=MAX_WORKERS,
                               help='Maximum number of buckets to process in parallel')
    migrate_parser.add_argument('--blacklist', dest='blacklist',
                               help='Comma-separated list of bucket name prefixes to exclude')
    migrate_parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    # Check bucket status command
    check_parser = subparsers.add_parser('check', help='Check status of a specific bucket')
    check_parser.add_argument('bucket_name', help='Name of the bucket to check')
    
    # Clean command to remove the database
    clean_parser = subparsers.add_parser('clean', help='Clean up the database files')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    
    # Set debug mode based on command line argument
    global DEBUG_MODE
    if hasattr(args, 'debug') and args.debug:
        DEBUG_MODE = True
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug mode enabled - detailed logging will be shown")
    
    # Initialize DB Connection
    init_db(DB_NAME)
    
    # Handle different commands
    if args.command == 'check':
        logger.info(f"Checking status for bucket: {args.bucket_name}")
        check_bucket_status(args.bucket_name)
        sys.exit(0)
    elif args.command == 'clean':
        logger.info("Cleaning up database files...")
        cleanup_db()
        sys.exit(0)
    elif args.command == 'migrate':
        # Update global constants if provided in arguments
        global DEST_BUCKET_SUFFIX, MAX_WORKERS
        if args.bucket_suffix:
            DEST_BUCKET_SUFFIX = args.bucket_suffix
        if args.max_workers:
            MAX_WORKERS = args.max_workers
            
        logger.info("--- Starting S3 Bucket Migration Script ---")
        
        # Get credentials and account IDs from command line arguments
        old_key = args.old_key
        old_secret = args.old_secret
        new_key = args.new_key
        new_secret = args.new_secret
        old_account_id = args.old_account_id
        new_account_id = args.new_account_id
        source_profile = args.source_profile
        dest_profile = args.dest_profile
        source_region = args.source_region
        dest_region = args.dest_region
        
        # Set blacklist from command line argument
        if args.blacklist:
            os.environ['BLACKLIST_BUCKET_STARTSWITH'] = args.blacklist
    else:
        # No command specified, show help
        parse_args.__globals__['parser'].print_help()
        sys.exit(1)

    # Check required credentials
    if not all([old_key, old_secret, new_key, new_secret]):
        logger.error("Missing required AWS credentials. Please set OLD_AWS_ACCESS_KEY_ID, OLD_AWS_SECRET_ACCESS_KEY, NEW_AWS_ACCESS_KEY_ID, and NEW_AWS_SECRET_ACCESS_KEY environment variables.")
        sys.exit(1)

    # Initialize DB Connection - get whether it was a new DB or existing one
    is_new_db = init_db(DB_NAME)

    # Reset incomplete tasks to ensure proper sequence - only if using existing DB
    if not is_new_db:
        reset_incomplete_tasks()

    # Create Boto3 Clients with specified regions or us-east-1 as default
    s3_client_old = get_boto_client(old_key, old_secret, region_name=source_region or 'us-east-1')
    s3_client_new = get_boto_client(new_key, new_secret, region_name=dest_region or 'us-east-1')

    # Check if policy copy should be done
    skip_policy = not all([old_account_id, new_account_id])
    if skip_policy:
        logger.info("AWS Account IDs not provided. Bucket policy copy will be skipped.")

    # Log region configuration
    if source_region:
        logger.info(f"Using configured source region: {source_region}")
    if dest_region:
        logger.info(f"Using configured destination region: {dest_region}")
    else:
        logger.info("No destination region specified. Buckets will be created in their source regions.")

    # Validate credentials
    if not validate_credentials(s3_client_old, "source account"):
        logger.error("Failed to validate source account credentials. Please check OLD_AWS_ACCESS_KEY_ID and OLD_AWS_SECRET_ACCESS_KEY.")
        sys.exit(1)

    if not validate_credentials(s3_client_new, "destination account"):
        logger.error("Failed to validate destination account credentials. Please check NEW_AWS_ACCESS_KEY_ID and NEW_AWS_SECRET_ACCESS_KEY.")
        sys.exit(1)

    # Discover source buckets
    logger.info("Discovering buckets in the old account...")
    try:
        response = s3_client_old.list_buckets()
        # Handle blacklist filtering properly
        blacklist_bucket_startswith = [pattern.strip() for pattern in os.environ.get('BLACKLIST_BUCKET_STARTSWITH', '').split(',') if pattern.strip()]
        logger.info(f"Bucket name prefixes in blacklist: {blacklist_bucket_startswith}")
        
        # Filter out blacklisted buckets
        source_buckets = [bucket['Name'] for bucket in response.get('Buckets', []) 
                          if not any(bucket['Name'].startswith(pattern) for pattern in blacklist_bucket_startswith)]
        logger.info(f"Found {len(source_buckets)} buckets in the old account after filtering: {source_buckets}")
    except ClientError as e:
        logger.error(f"Error listing buckets in old account: {e}. Check credentials/permissions.")
        sys.exit(1)
    except Exception as e:
         logger.error(f"Unexpected error listing buckets: {e}")
         sys.exit(1)


    # Add newly discovered buckets to the DB
    for bucket_name in source_buckets:
        dest_bucket_name = get_valid_bucket_name(bucket_name, DEST_BUCKET_SUFFIX)
        add_bucket_to_db(bucket_name, dest_bucket_name)

    # Get list of buckets to process (not completed)
    all_buckets_to_process = get_buckets_to_process()
    
    # Apply blacklist filter to buckets from database as well
    blacklist_bucket_startswith = [pattern.strip() for pattern in os.environ.get('BLACKLIST_BUCKET_STARTSWITH', '').split(',') if pattern.strip()]
    buckets_to_process = [
        (source, dest, region, status) for source, dest, region, status in all_buckets_to_process
        if not any(source.startswith(pattern) for pattern in blacklist_bucket_startswith)
    ]
    
    # Fix any incorrect destination bucket names in the database
    conn = get_db_connection()
    try:
        for source, dest, region, status in buckets_to_process:
            # Check if destination bucket name has incorrect suffix
            correct_dest = get_valid_bucket_name(source, DEST_BUCKET_SUFFIX)
            if dest != correct_dest:
                logger.info(f"Fixing incorrect destination bucket name: {dest} -> {correct_dest}")
                conn.execute(
                    "UPDATE buckets SET dest_bucket_name = ? WHERE source_bucket_name = ?",
                    (correct_dest, source)
                )
        conn.commit()
        
        # Refresh the list with corrected names
        buckets_to_process = [
            (source, get_valid_bucket_name(source, DEST_BUCKET_SUFFIX), region, status)
            for source, dest, region, status in buckets_to_process
        ]
    except sqlite3.Error as e:
        logger.error(f"DB Error fixing destination bucket names: {e}")
    
    if not buckets_to_process:
        logger.info("No buckets found requiring processing according to the database.")
        sys.exit(0)

    logger.info(f"Processing {len(buckets_to_process)} buckets (max {MAX_WORKERS} in parallel)...")

    # Process buckets in parallel
    tasks_submitted = 0
    tasks_completed = 0
    tasks_failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='S3Migrator') as executor:
        futures = {
            executor.submit(
                process_bucket,
                source_name, dest_name, region, status,
                s3_client_old, s3_client_new,
                old_account_id or '', new_account_id or '',  # Pass empty strings if None
                source_profile, dest_profile,
                dest_region,  # Pass destination region to process_bucket
            ): source_name
            for source_name, dest_name, region, status in buckets_to_process
        }
        tasks_submitted = len(futures)

        for future in as_completed(futures):
            source_bucket_name = futures[future]
            try:
                s_name, final_status = future.result()
                logger.info(f"Completed processing for {s_name} with final status: {final_status}")
                if final_status == STATUS_SYNC_COMPLETED:
                     tasks_completed += 1
                elif final_status == STATUS_FAILED:
                     tasks_failed += 1
                # Other statuses mean it was interrupted or handled mid-way

            except Exception as exc:
                logger.error(f"Bucket {source_bucket_name} generated an exception during future processing: {exc}")
                tasks_failed += 1
                # Attempt to mark as failed in DB, though process_bucket should have done this
                update_bucket_status(source_bucket_name, STATUS_FAILED, error=f"Executor exception: {exc}")


    logger.info("--- Migration Processing Finished ---")
    logger.info(f"Total buckets submitted for processing in this run: {tasks_submitted}")
    logger.info(f"Tasks completed successfully (reached '{STATUS_SYNC_COMPLETED}'): {tasks_completed}")
    logger.info(f"Tasks ending in 'failed' state: {tasks_failed}")
    
    # Only cleanup if all tasks completed successfully
    if tasks_failed == 0 and tasks_completed == tasks_submitted:
        logger.info("All tasks completed successfully. Cleaning up local database...")
        cleanup_db()
    else:
        logger.info("Some tasks failed or were incomplete. Keeping database for retry.")
        logger.info("Check logs and 's3_migration_state.db' for details on failures.")
        # Display error messages for failed buckets
        display_failed_bucket_errors()
        
        # Provide instructions for checking specific bucket status
        logger.info("To check a specific bucket's status, run: python aws_copier.py --check-bucket BUCKET_NAME")

def display_failed_bucket_errors():
    """Display detailed error messages for failed buckets from the database."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT source_bucket_name, dest_bucket_name, error_message, last_sync_output FROM buckets WHERE status = ?",
            (STATUS_FAILED,)
        )
        failed_buckets = cursor.fetchall()
        
        if not failed_buckets:
            logger.info("No failed buckets found in the database.")
            return
            
        logger.info(f"Found {len(failed_buckets)} failed buckets. Displaying error details:")
        
        for source, dest, error, sync_output in failed_buckets:
            logger.info(f"===== FAILED BUCKET: {source} (Dest: {dest}) =====")
            if error:
                logger.info(f"Error message: {error}")
            if sync_output:
                logger.info(f"Sync output: {sync_output}")
            logger.info("=" * 50)
            
    except sqlite3.Error as e:
        logger.error(f"DB Error fetching failed bucket details: {e}")

if __name__ == "__main__":
    main()