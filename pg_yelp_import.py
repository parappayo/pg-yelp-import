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


def add_review(db_connection, review):
    try:
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
    except errors.UniqueViolation:
        print('WARN: review already exists, id =', review['review_id'])


def parse_review_json(line):
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

    with open(review_filename, 'r') as infile:
        with connect(db_connection_string) as db_connection:
            for line in infile:
                add_review(db_connection, parse_review_json(line))
                db_connection.commit()
