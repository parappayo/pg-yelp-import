#
#  Methods that convert Yelp Dataset json docs into Postgres schema friendly dictionaries
#


import json, base64


def decode_id(id):
    return base64.b64decode(id + '==', '-_').hex()


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


def parse_checkin(line):
	doc = json.loads(line)
	return {
		'business_id' : decode_id(doc['business_id']),
		'checkins'    : doc['date'].split(',')
	}


def parse_tip(line):
    doc = json.loads(line)
    return {
        'user_id'          : decode_id(doc['user_id']),
        'business_id'      : decode_id(doc['business_id']),
        'tip_date'         : doc['date'],
        'compliment_count' : doc['compliment_count']
    }
