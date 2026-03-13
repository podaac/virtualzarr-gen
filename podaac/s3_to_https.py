#!/usr/bin/env python3
"""
Script to translate S3 URLs to the correct HTTPS URLs for NASA PODAAC data.
Handles both standard PODAAC and SWOT-specific endpoints.
"""

import json
import logging
import argparse

# Mapping of S3 buckets to their specific HTTPS endpoints
BUCKET_TO_HOST = {
    "podaac-swot-ops-cumulus-protected": "archive.swot.podaac.earthdata.nasa.gov",
    "podaac-swot-ops-cumulus-public": "archive.swot.podaac.earthdata.nasa.gov",
    # Add other mission-specific buckets here if they arise
}

DEFAULT_HOST = "archive.podaac.earthdata.nasa.gov"

def setup_logging(debug=False):
    """
    Configure logging for the application.

    Args:
        debug (bool): If True, sets logging level to DEBUG and enables
            debug logging for urllib3. Otherwise, sets level to INFO.
    """
    log_format = "%(asctime)s %(levelname)s %(message)s"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=log_format)
    if debug:
        logging.getLogger('urllib3').setLevel(logging.DEBUG)

def translate_s3_to_https(s3_url):
    """
    Translates an S3 URL to its HTTPS equivalent based on the bucket name.
    Example: s3://podaac-swot-ops-cumulus-protected/data.nc -> 
             https://archive.swot.podaac.earthdata.nasa.gov/podaac-swot-ops-cumulus-protected/data.nc
    """
    if not s3_url.startswith('s3://'):
        return s3_url

    # Remove 's3://' to work with the path
    raw_path = s3_url.replace('s3://', '')

    # Identify the bucket (the first part of the path)
    parts = raw_path.split('/', 1)
    bucket_name = parts[0]

    # Determine the correct host based on the bucket
    host = BUCKET_TO_HOST.get(bucket_name, DEFAULT_HOST)

    return f'https://{host}/{raw_path}'

def translate_filesystem_references(text):
    """
    Translates references in text, handling multiple possible bucket patterns.
    """
    if not isinstance(text, str):
        return text

    # Update filesystem class name
    text = text.replace('S3FileSystem', 'HTTPFileSystem')

    # Detect known PODAAC buckets in the string to replace them with full HTTPS paths
    # We look for common podaac bucket prefixes
    target_buckets = [
        "podaac-swot-ops-cumulus-protected",
        "podaac-swot-ops-cumulus-public",
        "podaac-ops-cumulus-protected",
        "podaac-ops-cumulus-public"
    ]

    for bucket in target_buckets:
        s3_prefix = f's3://{bucket}/'
        # Only replace if not already an HTTPS link
        if s3_prefix in text:
            host = BUCKET_TO_HOST.get(bucket, DEFAULT_HOST)
            https_prefix = f'https://{host}/{bucket}/'
            text = text.replace(s3_prefix, https_prefix)

        # Handle bare bucket references (common in Kerchunk 'templates' or metadata)
        # Check if the bucket is present but doesn't have the host prefix yet
        if bucket in text and 'https://' not in text:
            host = BUCKET_TO_HOST.get(bucket, DEFAULT_HOST)
            text = text.replace(bucket, f'https://{host}/{bucket}')

    return text

def process_kerchunk_refs(refs_dict):
    """
    Process a kerchunk refs dictionary and translate all S3 URLs to HTTPS.

    Args:
        refs_dict (dict): The 'refs' dictionary from a kerchunk JSON file

    Returns:
        dict: Updated refs dictionary with HTTPS URLs
    """
    updated_refs = {}
    for key, value in refs_dict.items():
        if isinstance(value, list) and len(value) >= 1:
            url = value[0]
            if isinstance(url, str) and url.startswith('s3://'):
                updated_refs[key] = [translate_s3_to_https(url)] + value[1:]
            else:
                updated_refs[key] = value
        elif isinstance(value, str):
            updated_refs[key] = translate_filesystem_references(value)
        else:
            updated_refs[key] = value
    return updated_refs

def convert_kerchunk_file(input_file, output_file=None):
    """
    Convert a kerchunk JSON file from S3 URLs to HTTPS URLs.

    Args:
        input_file (str): Path to input kerchunk JSON file
        output_file (str, optional): Path to output file. If None, overwrites input.
    """
    with open(input_file, 'r') as f:
        data = json.load(f)

    if 'refs' in data:
        data['refs'] = process_kerchunk_refs(data['refs'])

    output_path = output_file if output_file else input_file
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    logging.info("Converted %s -> %s", input_file, output_path)

def cli():
    """Main function to handle command-line usage."""
    parser = argparse.ArgumentParser(description="Translate PODAAC S3 URLs to appropriate HTTPS endpoints (SWOT or Standard).")
    parser.add_argument("input_file", help="Path to input kerchunk JSON file or S3 URL")
    parser.add_argument("output_file", nargs="?", default=None, help="Path to output file (optional)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    setup_logging(debug=args.debug)

    if args.input_file.startswith('s3://'):
        print(translate_s3_to_https(args.input_file))
    else:
        convert_kerchunk_file(args.input_file, args.output_file)

if __name__ == '__main__':
    cli()
