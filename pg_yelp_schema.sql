
CREATE SCHEMA IF NOT EXISTS yelp_academic_dataset;

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
