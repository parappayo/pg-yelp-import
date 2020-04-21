
CREATE SCHEMA IF NOT EXISTS yelp_academic_dataset;

CREATE TABLE IF NOT EXISTS yelp_academic_dataset.review_import (
	review_id		varchar(30) PRIMARY KEY,
	user_id			varchar(30),
	business_id		varchar(30),
	review_date		timestamp,
	stars			int,
	useful_count	int,
	funny_count		int,
	cool_count		int,
	review_text		text
);
