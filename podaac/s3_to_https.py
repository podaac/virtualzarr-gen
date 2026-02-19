#!/usr/bin/env python3
"""
Script to translate S3 URLs to HTTPS URLs for NASA PODAAC data.
Converts s3://podaac-ops-cumulus-protected/ to https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/
"""

import json
import logging
import argparse


# pylint: disable=function-redefined,R0801
def setup_logging(debug=False):
    """
    Configure logging for the application.

    Args:
        debug (bool): If True, sets logging level to DEBUG and enables
            debug logging for urllib3. Otherwise, sets level to INFO.
    """
    log_format = "%(asctime)s %(levelname)s %(message)s"
    if debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
        log = logging.getLogger('urllib3')
        log.setLevel(logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)


def translate_s3_to_https(s3_url):
    """
    Translate an S3 URL to its HTTPS equivalent.

    Args:
        s3_url (str): S3 URL in format s3://podaac-ops-cumulus-protected/...

    Returns:
        str: HTTPS URL in format https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/...
    """
    if not s3_url.startswith('s3://'):
        return s3_url

    # Remove s3:// prefix and prepend HTTPS base URL
    path = s3_url.replace('s3://', '')
    https_url = f'https://archive.podaac.earthdata.nasa.gov/{path}'

    return https_url


def translate_filesystem_references(text):
    """
    Translate filesystem references in text from S3 to HTTP.

    Args:
        text (str): Text that may contain S3FileSystem and s3:// references

    Returns:
        str: Text with HTTPFileSystem and https:// references
    """
    if not isinstance(text, str):
        return text

    # Replace S3FileSystem with HTTPFileSystem
    text = text.replace('S3FileSystem', 'HTTPFileSystem')

    # Replace s3:// URLs with https:// (both standalone and in paths)
    if 's3://' in text:
        text = text.replace(
            's3://podaac-ops-cumulus-protected/',
            'https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/'
        )

    # Also handle cases where the path appears without s3:// prefix
    # (e.g., after "File-like object HTTPFileSystem, podaac-ops-cumulus-protected/...")
    if 'podaac-ops-cumulus-protected/' in text and 'https://' not in text:
        text = text.replace(
            'podaac-ops-cumulus-protected/',
            'https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/'
        )

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
            # This is a reference to a file chunk [url, offset, size]
            url = value[0]
            if url.startswith('s3://'):
                # Translate S3 to HTTPS
                https_url = translate_s3_to_https(url)
                updated_refs[key] = [https_url] + value[1:]
            else:
                updated_refs[key] = value
        elif isinstance(value, str):
            # Process string values that might contain S3FileSystem or s3:// references
            updated_refs[key] = translate_filesystem_references(value)
        else:
            # Keep other entries as-is
            updated_refs[key] = value

    return updated_refs


def convert_kerchunk_file(input_file, output_file=None):
    """
    Convert a kerchunk JSON file from S3 URLs to HTTPS URLs.

    Args:
        input_file (str): Path to input kerchunk JSON file
        output_file (str, optional): Path to output file. If None, overwrites input.
    """
    # Read the input file
    with open(input_file, 'r') as f:
        data = json.load(f)

    # Process the refs
    if 'refs' in data:
        data['refs'] = process_kerchunk_refs(data['refs'])

    # Write to output file
    output_path = output_file if output_file else input_file
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    logging.info("Converted %s -> %s", input_file, output_path)
    logging.info("Translated S3 URLs to HTTPS URLs")


def cli():
    """Main function to handle command-line usage."""
    parser = argparse.ArgumentParser(description="Translate S3 URLs to HTTPS URLs for NASA PODAAC data.")
    parser.add_argument("input_file", help="Path to input kerchunk JSON file or S3 URL")
    parser.add_argument("output_file", nargs="?", default=None, help="Path to output file (optional)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    setup_logging(debug=args.debug)

    input_arg = args.input_file
    output_file = args.output_file

    # Check if input is a URL or a file
    if input_arg.startswith('s3://'):
        # Single URL translation
        https_url = translate_s3_to_https(input_arg)
        logging.info("S3:    %s", input_arg)
        logging.info("HTTPS: %s", https_url)
    else:
        # File conversion
        convert_kerchunk_file(input_arg, output_file)


if __name__ == '__main__':
    cli()
