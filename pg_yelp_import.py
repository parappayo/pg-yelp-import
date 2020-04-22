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


def insert_record(db_cursor, schema, table, record_dict):
    query = get_insert_query(schema, table, record_dict.keys(), record_dict.values())
    db_cursor.execute(query)


def get_insert_func(table):
    return lambda db_cursor, record_dict: insert_record(db_cursor, 'yelp_academic_dataset', table, record_dict)


def process_file(input_file, db_connection, parse_func, insert_func, log_func):
    with db_connection.cursor() as db_cursor:
        line_count = 0
        for line in input_file:
            try:
                insert_func(db_cursor, parse_func(line))
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


if __name__ == '__main__':
    jobs = [
        (   'yelp_academic_dataset_user.json',
            parse_user,
            get_insert_func('user'),
            "inserted {:,d} users"),

        # (   'yelp_academic_dataset_user.json',
        #     parse_friends,
        #     insert_friends,
        #     "processed friends for {:,d} users"),

        (   'yelp_academic_dataset_business.json',
            parse_business,
            get_insert_func('business'),
            "inserted {:,d} businesses"),

        (   'yelp_academic_dataset_review.json',
            parse_review,
            get_insert_func('review'),
            "inserted {:,d} reviews"),

        (   'yelp_academic_dataset_checkin.json',
            parse_checkin,
            insert_checkin,
            "processed check-ins for {:,d} businesses"),

        (   'yelp_academic_dataset_tip.json',
            parse_tip,
            get_insert_func('tip'),
            "inserted {:,d} tips"),
    ]

    db_connection_string = ''
    with open('connection_string.txt', 'r') as infile:
        db_connection_string = infile.read()

    with connect(db_connection_string) as db_connection:
        for job in jobs:
            process_job(db_connection, *job)
