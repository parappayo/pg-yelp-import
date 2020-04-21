import sys, json
from psycopg2 import connect, sql, errors


def get_insert_query(schema, table, fields, values):
    query = sql.SQL(
        "INSERT INTO {schema}.{table} ({fields}) VALUES ({values})")

    query = query.format(
        schema = sql.Identifier(schema),
        table = sql.Identifier(table),
        fields = sql.SQL(',').join(sql.Identifier(f) for f in fields),
        values = sql.SQL(',').join(sql.Literal(v) for v in values))

    # can debug the query by outputting it like so
    # print(query.as_string(db_cursor))
    return query


def insert_record(db_connection, schema, table, fields, record_dict):
    db_cursor = db_connection.cursor()
    query = get_insert_query(schema, table, fields, (record_dict[f] for f in fields))
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


def insert_review(db_connection, review):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'review_import',
        [   'review_id',
            'user_id',
            'business_id',
            'review_date',
            'stars',
            'useful_count',
            'funny_count',
            'cool_count',
            'review_text' ],
        review)


def parse_review(line):
    doc = json.loads(line)
    return {
        'review_id' : doc['review_id'],
        'user_id' : doc['user_id'],
        'business_id' : doc['business_id'],
        'review_date' : doc['date'],
        'stars' : doc['stars'],
        'useful_count' : doc['useful'],
        'funny_count' : doc['funny'],
        'cool_count' : doc['cool'],
        'review_text' : doc['text']
    }


if __name__ == '__main__':
    db_connection_string = ''
    review_filename = 'yelp_academic_dataset_review.json'

    with open('connection_string.txt', 'r') as infile:
        db_connection_string = infile.read()

    with open(review_filename, 'r', encoding='utf-8') as infile:
        with connect(db_connection_string) as db_connection:
            process_file(
                infile,
                db_connection,
                parse_review,
                insert_review,
                lambda line_count:
                    print("Inserted {:,d} reviews".format(line_count), flush=True))
