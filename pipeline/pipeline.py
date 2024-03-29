"""
ETL pipeline that takes kiosk data from museum S3 bucket
and transforms it into a psql database.
"""

from os import environ
import argparse
from csv import reader
import logging

from dotenv import load_dotenv

from psycopg2 import connect, OperationalError
from psycopg2.extras import execute_values, RealDictCursor

from extract import (download_specific_files,
                     delete_csv_files,
                     get_s3_client,
                     merge_csv_to_file)


def argparse_is_my_friend():
    """Set up argparse to pass arguments automatically in command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", "-b", type=str,
                        help="specify the AWS bucket you are accessing.")
    parser.add_argument("--num_rows", "-nr", type=int,
                        help="number of instance rows to be uploaded to database")
    parser.add_argument("--log", "-l", default=False, action='store_true',
                        help="boolean argument to set log to output in console or file")

    args = vars(parser.parse_args())
    return (args.get('bucket'), args.get('num_rows'), args.get('log'))


def log_to_file():
    """Outputs the logs to a file instead of the command line"""
    logging.basicConfig(filename='pipeline.log', encoding='utf-8',
                        format='%(asctime)s %(levelname)s %(message)s')


def get_db_connection():
    """
    Gets a connection to the specified database
    """
    load_dotenv()
    try:
        conn = connect(
            user=environ["DATABASE_USERNAME"],
            password=environ["DATABASE_PASSWORD"],
            host=environ["DATABASE_IP"],
            port=environ["DATABASE_PORT"],
            database=environ["DATABASE_NAME"],
            cursor_factory=RealDictCursor)
        logging.info('Connected to database successfully')
        return conn
    except OperationalError as err:
        logging.error('Connection attempt to database unsuccessful. %s', err)
        return None


def get_cursor(conn) -> list[dict[str, str]]:
    """Gets a cursor to browse database"""
    return conn.cursor(cursor_factory=RealDictCursor)


def load_kiosk_data(file_path: str) -> list:
    """Loads the merged csv file generated by kiosks"""
    with open(f'{file_path}/lmnh_merged_hist_data.csv', 'r',
              encoding='utf-8') as file:
        data = list(reader(file))
    logging.info('Kiosk data successfully acquired.')
    return data[1:]


def get_rating_instances(kiosk_data: list[list]) -> list[list]:
    """Get a list of rating instances"""
    logging.info('Rating instances obtained.')
    return [instance for instance in kiosk_data if int(instance[2]) >= 0]


def format_rating_instances(ratings: list[list]) -> list[list]:
    """Format the rating instances to return sql-friendly data."""
    for row in ratings:
        del row[3]
        row[1] = int(row[1]) + 1
        row[2] = int(row[2]) + 1
    logging.info('Rating instances formatted.')
    return ratings


def upload_rating_instances(conn, formatted_ratings: list[list], num_rows: int | None):
    """
    Upload the formatted rating instances to a database,
    given a number of rows.
    """
    sql_query = """
    INSERT INTO rating_instance
        (rating_created_at, exhibition_id, rating_type_id)
    VALUES
    %s;
    """

    try:
        curr = get_cursor(conn)
        selected_rows = formatted_ratings[:
                                          num_rows] if num_rows else formatted_ratings
        execute_values(curr, sql_query, selected_rows)
        conn.commit()
        logging.info('Uploaded rating instances to the database.')
    except AttributeError:
        logging.error(
            'Cursor was not created successfully, database not updated.')


def get_support_instances(kiosk_data: list[list]) -> list[list]:
    """Get a list of support instances"""
    logging.info('Support instances obtained.')
    return [instance for instance in kiosk_data if int(instance[2]) < 0]


def format_support_instances(supports: list[list]) -> list[list]:
    """Format the support instances to return sql-friendly data."""
    for row in supports:
        del row[2]
        row[1] = int(row[1]) + 1
        # third column becomes type after deleting val
        row[2] = int(float(row[2])) + 1
    logging.info('Support instances formatted.')
    return supports


def upload_support_instances(conn, formatted_supports: list[list], num_rows: int | None):
    """
    Upload the formatted support instances to a database,
    given a number of rows.
    """
    sql_query = """
    INSERT INTO support_instance
        (instance_created_at, exhibition_id, support_type_id)
    VALUES
    %s;
    """

    try:
        curr = get_cursor(conn)
        selected_rows = formatted_supports[:
                                           num_rows] if num_rows else formatted_supports
        execute_values(curr, sql_query, selected_rows)
        conn.commit()
        logging.info('Uploaded support instances to the database.')
    except AttributeError:
        logging.error(
            'Cursor was not created successfully, database not updated.')


def main():
    """Run the pipeline using the associated functions"""

    (arg_bucket, arg_num_rows,
     arg_log_to_file) = argparse_is_my_friend()

    if arg_log_to_file:
        log_to_file()

    load_dotenv()

    bucket = arg_bucket if arg_bucket else environ["MUSEUM_BUCKET"]

    s3 = get_s3_client()

    download_specific_files(
        s3, bucket, 'lmnh', 'museum_files')

    merge_csv_to_file("lmnh_hist_data", "museum_files/")

    delete_csv_files("lmnh_hist_data", "museum_files/")

    conn = get_db_connection()
    kiosk_data = load_kiosk_data("museum_files")
    rating_instances = get_rating_instances(kiosk_data)
    formatted_rating_instances = format_rating_instances(rating_instances)
    upload_rating_instances(conn, formatted_rating_instances, arg_num_rows)
    support_instances = get_support_instances(kiosk_data)
    formatted_support_instances = format_support_instances(support_instances)
    upload_support_instances(conn, formatted_support_instances, arg_num_rows)

    conn.close()


if __name__ == "__main__":
    main()
