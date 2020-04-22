import sys
from psycopg2 import connect, sql, errors
from pg_yelp_parse import *


def get_insert_query(schema, table, fields, values):
    query = sql.SQL(
        "INSERT INTO {schema}.{table} ({fields}) VALUES ({values}) ON CONFLICT DO NOTHING")

    query = query.format(
        schema = sql.Identifier(schema),
        table = sql.Identifier(table),
        fields = sql.SQL(',').join(sql.Identifier(f) for f in fields),
        values = sql.SQL(',').join(sql.Literal(v) for v in values))

    # can debug the query by outputting it like so
    # print(query.as_string(db_cursor))
    return query


def insert_record(db_connection, schema, table, record_dict):
    query = get_insert_query(schema, table, record_dict.keys(), record_dict.values())
    db_cursor.execute(query)


def process_file(input_file, db_connection, parse_func, insert_func, log_func):
    line_count = 0
    for line in input_file:
        try:
            insert_func(db_connection, parse_func(line))
        except errors.UniqueViolation:
            continue

        line_count += 1
        if line_count % 10000 == 0:
            db_connection.commit()
            log_func(line_count)
    log_func(line_count)


def process_job(db_connection, input_filename, parse_func, insert_func, log_format):
    with open(input_filename, 'r', encoding='utf-8') as infile:
        with db_connection:
            process_file(
                infile,
                db_connection,
                parse_func,
                insert_func,
                lambda line_count:
                    print(log_format.format(line_count), flush=True))


def insert_user(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'user',
        record_dict)


def insert_business(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'business',
        record_dict)


def insert_review(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'review',
        record_dict)


def insert_checkin(db_connection, record_dict):
    for checkin in record_dict['checkins']:
        insert_record(
            db_connection,
            'yelp_academic_dataset',
            'checkin',
            {
                'business_id' : record_dict['business_id'],
                'checkin_date' : checkin
            })


def insert_tip(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'tip',
        record_dict)


if __name__ == '__main__':
    jobs = [
        (   'yelp_academic_dataset_user.json',
            parse_user,
            insert_user,
            "inserted {:,d} users"),

        # (   'yelp_academic_dataset_user.json',
        #     parse_friends,
        #     insert_friends,
        #     "processed friends for {:,d} users"),

        (   'yelp_academic_dataset_business.json',
            parse_business,
            insert_business,
            "inserted {:,d} businesses"),

        (   'yelp_academic_dataset_review.json',
            parse_review,
            insert_review,
            "inserted {:,d} reviews"),

        (   'yelp_academic_dataset_checkin.json',
            parse_checkin,
            insert_checkin,
            "processed check-ins for {:,d} businesses"),

        (   'yelp_academic_dataset_tip.json',
            parse_tip,
            insert_tip,
            "inserted {:,d} tips"),
    ]

    db_connection_string = ''
    with open('connection_string.txt', 'r') as infile:
        db_connection_string = infile.read()

    db_connection = connect(db_connection_string)
    db_cursor = db_connection.cursor()
    try:
        for job in jobs:
            process_job(db_connection, *job)
    finally:
        db_cursor.close()
        db_connection.close()
