"""
Museum data extraction script
Collect and clean data from an S3 bucket associated with the museum
"""

from os import (environ,
                listdir,
                remove)
import logging

from dotenv import load_dotenv

from boto3 import client
import pandas as pd


def get_s3_client() -> client:
    """Create an s3 client to access s3 buckets."""
    load_dotenv()

    s3 = client("s3",
                aws_access_key_id=environ["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=environ["AWS_SECRET_ACCESS_KEY"])
    return s3


def get_bucket_names(s3_client) -> list[str]:
    """Returns a list of available bucket names."""

    response = s3_client.list_buckets()

    buckets = response["Buckets"]

    logging.info('Bucket names retrieved.')

    return [b["Name"] for b in buckets]


def get_bucket_objects(s3_client, bucket_name: str) -> list[str]:
    """Return a list of available objects in a bucket."""

    response = s3_client.list_objects(Bucket=bucket_name)

    objects = response['Contents']

    logging.info('Bucket objects retrieved.')

    return [o["Key"] for o in objects]


def download_all_files(s3_client, bucket_name: str, folder_name: str = "data") -> None:
    """Downloads all files from a bucket to a named folder"""

    objects = get_bucket_objects(s3_client, "sigma-resources-museum")

    for o in objects:
        s3_client.download_file(bucket_name,
                                o,
                                f"{folder_name}/{o}")

    logging.info('All bucket objects downloaded.')


def download_specific_files(s3_client, bucket_name: str,
                            filter_by: str, folder_name: str = "data") -> None:
    """Downloads specific files from a bucket to a named folder"""

    objects = get_bucket_objects(s3_client, "sigma-resources-museum")
    filtered_objects = [
        relevant for relevant in objects if relevant.startswith(filter_by)]

    if filtered_objects:
        for o in filtered_objects:
            if o.endswith('.csv') or o.endswith('.json'):
                s3_client.download_file(bucket_name,
                                        o,
                                        f"{folder_name}/{o}")
        logging.info('Selected bucket objects downloaded.')
    else:
        logging.error("Museum data folder does not consist of relevant data")


def merge_csv_to_file(filter_by: str, folder_path: str):
    """Merge multiple csvs downloaded into one combined csv"""
    csv_list = listdir(folder_path)
    filtered_csv_list = [
        relevant for relevant in csv_list if relevant.startswith(filter_by)]

    combined_csv = pd.concat([pd.read_csv(f"{folder_path}/{f}")
                             for f in filtered_csv_list])
    combined_csv.to_csv(f"{folder_path}/lmnh_merged_hist_data.csv",
                        index=False, encoding='utf-8-sig')

    logging.info('CSV files merged successfully as lmnh_merged_hist_data.csv.')


def delete_csv_files(filter_by: str, folder_path: str):
    """Remove csvs files that are not needed"""
    csv_list = listdir(folder_path)
    filtered_csv_list = [
        relevant for relevant in csv_list
        if relevant.startswith(filter_by) and relevant.endswith(".csv")]

    for file in filtered_csv_list:
        if "merged" not in file:
            remove(f"{folder_path}/{file}")

    logging.info('Obsolete CSV files deleted successfully.')


if __name__ == "__main__":

    load_dotenv()

    s3 = get_s3_client()

    download_specific_files(
        s3, "sigma-resources-museum", 'lmnh', 'museum_files')

    merge_csv_to_file("lmnh_hist_data", "museum_files/")

    delete_csv_files("lmnh_hist_data", "museum_files/")
