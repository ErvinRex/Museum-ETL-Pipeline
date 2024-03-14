"""
Museum data consume script
Collect and clean data from a Kafka cluster associated with the museum
"""
from os import environ as ENV
import logging
from psycopg2.extras import RealDictCursor
from psycopg2 import connect, OperationalError
from confluent_kafka import Consumer
from dotenv import load_dotenv
import json
from cleaning import clean_data

logging.basicConfig(filename='consume_logs.txt', encoding='utf-8', level=logging.INFO,
                    format='%(asctime)s -- %(name)s -- %(levelname)s -- %(message)s',
                    filemode='w',)

logging.getLogger().setLevel(logging.INFO)

VALID_TYPES = [0, 1]


def get_db_connection():
    """
    Gets a connection to the specified database
    """
    load_dotenv()
    try:
        conn = connect(
            user=ENV["DATABASE_USERNAME"],
            password=ENV["DATABASE_PASSWORD"],
            host=ENV["DATABASE_IP"],
            port=ENV["DATABASE_PORT"],
            database=ENV["DATABASE_NAME"],
            cursor_factory=RealDictCursor)
        logging.info('Connected to database successfully')
        return conn
    except OperationalError as err:
        logging.error('Connection attempt to database unsuccessful. %s', err)
        return None


def get_cursor(conn) -> list[dict[str, str]]:
    """Gets a cursor to browse database"""
    return conn.cursor(cursor_factory=RealDictCursor)


def format_support_instance(loaded_data):
    """Obtain each key value for support instance to be inputted in RDS"""

    at = loaded_data.get("at")
    site = loaded_data.get("site")
    type = loaded_data.get("type")

    return at, site, type


def upload_support_instance(conn, loaded_data):
    """Upload support instance cleaned data to an AWS RDS"""

    at, site, type = format_support_instance(loaded_data)

    sql_query = """
    INSERT INTO support_instance
        (instance_created_at, exhibition_id, support_type_id)
    VALUES
    (%s, %s, %s);
    """

    try:
        curr = get_cursor(conn)
        curr.execute(sql_query, (at, (int(site) + 1), (int(type) + 1)))
        conn.commit()
        logging.info('Uploaded support instance to the database.')
    except AttributeError:
        logging.error(
            'Cursor was not created successfully, database not updated.')


def format_rating_instance(loaded_data):
    """Obtain each key value for rating instance to be inputted in RDS"""

    at = loaded_data.get("at")
    site = loaded_data.get("site")
    val = loaded_data.get("val")

    return at, site, val


def upload_rating_instance(conn, loaded_data):
    """Upload rating instance cleaned data to an AWS RDS"""

    at, site, val = format_rating_instance(loaded_data)

    sql_query = """
    INSERT INTO rating_instance
        (rating_created_at, exhibition_id, rating_type_id)
    VALUES
    (%s, %s, %s);
    """

    try:
        curr = get_cursor(conn)
        curr.execute(sql_query, (at, int(site) + 1, int(val) + 1))
        conn.commit()
        logging.info('Uploaded rating instance to the database.')
    except AttributeError:
        logging.error(
            'Cursor was not created successfully, database not updated.')


def select_data_upload(conn, loaded_data):
    """Select upload function to upload message to AWS RDS"""
    type = loaded_data.get('type', None)
    if type in VALID_TYPES:
        upload_support_instance(conn, loaded_data)
    else:
        upload_rating_instance(conn, loaded_data)


def consume_messages(conn, consumer: Consumer):
    """Intake messages from a Kafka cluster"""
    msg_num = 0
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                logging.error("ERROR: %s", msg.error())
            if clean_data(msg) is True:
                data = msg.value().decode()
                loaded_data = json.loads(data)

                select_data_upload(conn, loaded_data)
                msg_num += 1

                logging.info(f'Message {msg_num} log: {data}')

    except KeyboardInterrupt as err:
        logging.error('Consuming period cancelled %s', err)
    finally:
        consumer.close()


def main():
    """Run the consume pipeline using the associated functions"""

    load_dotenv()

    conn = get_db_connection()

    kafka_config = {
        'bootstrap.servers': ENV["BOOTSTRAP_SERVERS"],
        'security.protocol': ENV["SECURITY_PROTOCOL"],
        'sasl.mechanisms': ENV["SASL_MECHANISM"],
        'sasl.username': ENV["USERNAME"],
        'sasl.password': ENV["PASSWORD"],
        'group.id': ENV["GROUP"],
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(kafka_config)
    consumer.subscribe([ENV["TOPIC"]])

    consume_messages(conn, consumer)


if __name__ == "__main__":
    main()
