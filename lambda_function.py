import os
import tempfile
import logging
import ast
from PIL import Image
import boto3

s3 = boto3.client('s3')
DEST_BUCKET = os.environ['DEST_BUCKET']
SIZE = ast.literal_eval(os.getenv('THUMBNAIL_SIZE', '(128, 128)'))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Starting thumbnail generation")
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        if not key.lower().endswith(('jpg', 'jpeg', 'png')):
            logger.info(f"Skipping non-image file: {key}")
            continue

        if key.startswith('thumb-'):
            logger.info(f"Skipping already processed thumbnail: {key}")
            continue

        thumb = 'thumb-' + key
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                download_path = os.path.join(tmpdir, key)
                upload_path = os.path.join(tmpdir, thumb)

                s3.download_file(source_bucket, key, download_path)
                logger.info(f"Downloaded {key} from bucket {source_bucket}")

                generate_thumbnail(download_path, upload_path)
                logger.info(f"Thumbnail generated for {key}")

                s3.upload_file(upload_path, DEST_BUCKET, thumb)
                logger.info(f"Thumbnail uploaded to {DEST_BUCKET}/{thumb}")
            except Exception as e:
                logger.error(f"Error processing file {key}: {e}")

def generate_thumbnail(source_path, dest_path):
    logger.info(f"Generating thumbnail from: {source_path}")
    with Image.open(source_path) as image:
        image.thumbnail(SIZE)
        image.save(dest_path)

# yenarievilo_aula_udemy
