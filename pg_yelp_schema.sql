
CREATE SCHEMA IF NOT EXISTS yelp_academic_dataset;

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.user
(
	user_id				uuid PRIMARY KEY,
	name				text,
	review_count		int,
	yelping_since		timestamp,
	useful_count		int,
	funny_count			int,
	cool_count			int,
	elite_years			text,
	fans_count			int,
	average_stars		real,
	compliment_hot_count	int,
	compliment_more_count	int,
	compliment_profile_count	int,
	compliment_cute_count	int,
	compliment_list_count	int,
	compliment_note_count	int,
	compliment_plain_count	int,
	compliment_cool_count	int,
	compliment_funny_count	int,
	compliment_writer_count	int,
	compliment_photos_count	int
);

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.friends
(
	user_id		uuid REFERENCES yelp_academic_dataset.user,
	friend_id	uuid REFERENCES yelp_academic_dataset.user
);

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.business
(
	business_id		uuid PRIMARY KEY,
	name			text,
	address			text,
	city			text,
	state			text,
	postal_code		text,
	categories		text,
	latitude		real,
	longitude		real,
	stars			real,
	review_count	int,
	is_open			int,
	attributes		jsonb,
	hours			jsonb
);

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.review
(
	review_id		uuid PRIMARY KEY,
	user_id			uuid,
	business_id		uuid REFERENCES yelp_academic_dataset.business,
	review_date		timestamp,
	stars			real,
	useful_count	int,
	funny_count		int,
	cool_count		int,
	review_text		text
);

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.checkin
(
	business_id		uuid REFERENCES yelp_academic_dataset.business,
	checkin_date	timestamp
);
