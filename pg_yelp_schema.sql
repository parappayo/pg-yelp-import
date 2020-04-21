
CREATE SCHEMA IF NOT EXISTS yelp_academic_dataset;

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.business
(
	business_id		varchar(30) PRIMARY KEY,
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
	review_id		varchar(30) PRIMARY KEY,
	user_id			varchar(30),
	business_id		varchar(30),
	review_date		timestamp,
	stars			real,
	useful_count	int,
	funny_count		int,
	cool_count		int,
	review_text		text
);
