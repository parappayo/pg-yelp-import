import sys, json
from psycopg2 import connect, sql


# TODO: fetch db name, username, password from local secrets
db_connection_string = "dbname='postgres' user='postgres' host='localhost' password='adminpass'"


def add_review(db_connection, review):
    db_cursor = db_connection.cursor()

    schema = 'yelp_academic_dataset'
    table = 'review_import'
    fields = [
        'review_id',
        'user_id',
        'business_id',
        'review_date',
        'stars',
        'useful_count',
        'funny_count',
        'cool_count',
        'review_text' ]
    values = ( review[f] for f in fields )

    query = sql.SQL(
        "INSERT INTO {schema}.{table} ({fields}) VALUES ({values})")
    query = query.format(
        schema = sql.Identifier(schema),
        table = sql.Identifier(table),
        fields = sql.SQL(',').join(sql.Identifier(f) for f in fields),
        values = sql.SQL(',').join(sql.Literal(v) for v in values))

    # can debug the query by outputting it like so
    # print(query.as_string(db_cursor))

    db_cursor.execute(query)


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
    input_filename = 'yelp_academic_dataset_review.json'

    with open(input_filename, 'r') as infile:
        with connect(db_connection_string) as db_connection:
            for line in infile:
                add_review(db_connection, parse_review_json(line))
                db_connection.commit()
