import sys, json, base64
from psycopg2 import connect, sql, errors


def decode_id(id):
    return base64.b64decode(id + '==', '-_').hex()


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


def insert_record(db_connection, schema, table, record_dict):
    with db_connection:
        with db_connection.cursor() as db_cursor:
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


def parse_user(line):
    doc = json.loads(line)
    return {
        'user_id' : decode_id(doc['user_id']),
        'name' : doc['name'],
        'review_count' : doc['review_count'],
        'yelping_since' : doc['yelping_since'],
        'useful_count' : doc['useful'],
        'funny_count' : doc['funny'],
        'cool_count' : doc['cool'],
        'elite_years' : doc['elite'],
        'fans_count' : doc['fans'],
        'average_stars' : doc['average_stars'],
        'compliment_hot_count' : doc['compliment_hot'],
        'compliment_more_count' : doc['compliment_more'],
        'compliment_profile_count' : doc['compliment_profile'],
        'compliment_cute_count' : doc['compliment_cute'],
        'compliment_list_count' : doc['compliment_list'],
        'compliment_note_count' : doc['compliment_note'],
        'compliment_plain_count' : doc['compliment_plain'],
        'compliment_cool_count' : doc['compliment_cool'],
        'compliment_funny_count' : doc['compliment_funny'],
        'compliment_writer_count' : doc['compliment_writer'],
        'compliment_photos_count' : doc['compliment_photos'],
    }


def insert_user(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'user',
        record_dict)


def parse_business(line):
    doc = json.loads(line)
    return {
        'business_id'  : decode_id(doc['business_id']),
        'name'         : doc['name'],
        'address'      : doc['address'],
        'city'         : doc['city'],
        'state'        : doc['state'],
        'postal_code'  : doc['postal_code'],
        'categories'   : doc['categories'],
        'latitude'     : doc['latitude'],
        'longitude'    : doc['longitude'],
        'stars'        : doc['stars'],
        'review_count' : doc['review_count'],
        'is_open'      : doc['is_open'],
        'attributes'   : json.dumps(doc['attributes']),
        'hours'        : json.dumps(doc['hours'])
    }


def insert_business(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'business',
        record_dict)


def parse_review(line):
    doc = json.loads(line)
    return {
        'review_id'    : decode_id(doc['review_id']),
        'user_id'      : decode_id(doc['user_id']),
        'business_id'  : decode_id(doc['business_id']),
        'review_date'  : doc['date'],
        'stars'        : doc['stars'],
        'useful_count' : doc['useful'],
        'funny_count'  : doc['funny'],
        'cool_count'   : doc['cool'],
        'review_text'  : doc['text']
    }


def insert_review(db_connection, record_dict):
    insert_record(
        db_connection,
        'yelp_academic_dataset',
        'review',
        record_dict)


def insert_checkin(db_connection, record_dict):
    business = decode_id(record_dict['business_id'])
    checkins = record_dict['date'].split(',')

    for checkin in checkins:
        insert_record(
            db_connection,
            'yelp_academic_dataset',
            'checkin',
            {   'business_id' : business,
                'checkin_date' : checkin })


def parse_tip(line):
    doc = json.loads(line)
    return {
        'user_id'          : decode_id(doc['user_id']),
        'business_id'      : decode_id(doc['business_id']),
        'tip_date'         : doc['date'],
        'compliment_count' : doc['compliment_count']
    }


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
            lambda line: json.loads(line),
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
