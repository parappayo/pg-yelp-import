# pg-yelp-import

Scripts to import [Yelp Academic Dataset](https://www.yelp.com/dataset) into [Postgres](https://www.postgresql.org/).

Working with 10 GB of data as a handful of newline delimited files containing json docs can be unwieldy. Having the data as Postgres tables makes it easier to filter, summarize, and further transform the data set.

## Caveats

This project is for my own personal learning purposes only. Do not use this in a professional or production context. Be skeptical of the code.

The structure of the Yelp Dataset is likely to change over time and this project is unlikely to be maintained. Do not expect anything to work.

## Tech Stack

* [Python 3](https://www.python.org/) with [psycopg2](https://www.psycopg.org/)
* PostgreSQL 11

## Setup

* Download the Yelp Dataset and extract into json files.
  * You may need to rename the file extension from `.tar` to `.tgz`.
* Install the required Python libs.
  * `pipenv install`
* Create the Postgres tables.
  * `psql -h localhost -p 5432 -U postgres -f pg_yelp_schema.sql`
  * Your connection details may vary.
* Set the database credentials.
  * `echo "dbname='postgres' user='postgres' host='localhost' password='adminpass'" >connection_string.txt`
  * Your connection details may vary.

## Usage

* Count up how many records need to be inserted in total.
  * `wc -l *.json`
* Run the import script.
  * `python3 pg_yelp_import.py`

It will log every 10k inserts to give you a rough idea of progress.

During development, I tested with Yelp Dataset that has over 11.6 million records.

```
$ wc -l *.json
     209393 yelp_academic_dataset_business.json
     175187 yelp_academic_dataset_checkin.json
    8021122 yelp_academic_dataset_review.json
    1320761 yelp_academic_dataset_tip.json
    1968703 yelp_academic_dataset_user.json
   11695166 total
```

### Customizations

I made the decision to enforce some basic foreign key constraints for better data consistency. These are easy enough to relax; just customize the schema QA in `pg_yelp_schema.sql` before creating tables. Look for the `REFERENCES` clauses and remove them as needed.

It may be useful to run partial import jobs by customizing the set of steps under `pg_yelp_import.py`. Look for the list of tuples called `jobs` and comment out the ones corresponding to tables that you do not want to populate. Skipping tables that you don't care about can save a lot of time.

## What I Learned

* Postgres has a nice On Conflict Clause feature to implement upserts.
  * `INSERT ... ON CONFLICT DO UPDATE`
* Catching a `UniqueViolation` exception is not enough when doing multiple inserts per transaction.
  * The next insert after a uniqueness constraint violation will throw `InFailedSqlTransaction`.
  * Work-around: `INSERT ... ON CONFLICT DO NOTHING`
* There is a quick and dirty conversion from 22 character Base64 strings to UUIDs.
  * `base64.b64decode(id + '==', '-_').hex()`
* My SQL could be better. Some fun SQL features to play with include,
  * [Upserts](https://www.postgresqltutorial.com/postgresql-upsert/)
  * [Common Table Expressions](https://www.postgresql.org/docs/11/queries-with.html)
  * [Materialized Views](https://www.postgresql.org/docs/11/rules-materializedviews.html)

## Demos

### Popular Businesses

```
select name, city, stars, review_count
from yelp_academic_dataset.business
where review_count > 50
order by stars desc
limit 10;
```

```
"Keratin & Color by Vanessa Mills"	"Las Vegas"	"5"	53
"American Home Water & Air"	"Phoenix"	"5"	340
"Phoenix Carpet Repair & Cleaning"	"Phoenix"	"5"	111
"One Shot Installation"	"Peoria"	"5"	132
"Elite Floor & Furniture Cleaning"	"Tempe"	"5"	71
"Deana Phelps Hair Designs"	"Scottsdale"	"5"	55
"Acupuncture Vegas"	"Las Vegas"	"5"	56
"KOLI Equestrian Center"	"Phoenix"	"5"	59
"Veghed"	"Toronto"	"5"	71
"The Real Crepe"	"Las Vegas"	"5"	205
```

```
select name, city, stars, review_count
from yelp_academic_dataset.business
order by review_count desc
limit 10;
```

```
"Bacchanal Buffet"	"Las Vegas"	"4"	10129
"Mon Ami Gabi"	"Las Vegas"	"4"	9264
"Wicked Spoon"	"Las Vegas"	"3.5"	7383
"Hash House A Go Go"	"Las Vegas"	"4"	6751
"Gordon Ramsay BurGR"	"Las Vegas"	"4"	5494
"Earl of Sandwich"	"Las Vegas"	"4.5"	5232
"Yardbird Southern Table & Bar"	"Las Vegas"	"4.5"	4828
"Secret Pizza"	"Las Vegas"	"4"	4803
"The Buffet At Wynn"	"Las Vegas"	"3.5"	4803
"The Cosmopolitan of Las Vegas"	"Las Vegas"	"4"	4740
```

### Finer Grained Average Stars

The `business.stars` values are rounded to increments of `0.5`. We can derive more accuracy by averaging the data in the `review` table.

```
create materialized view if not exists yelp_academic_dataset.business_avg_stars as
(
	select business_id
	,avg(stars) as avg_stars
	from yelp_academic_dataset.review
	group by business_id
);
```

```
select b.name
,b.city
,round(bas.avg_stars::numeric, 2) as avg_stars
,b.review_count
from yelp_academic_dataset.business b
join yelp_academic_dataset.business_avg_stars bas
  on b.business_id = bas.business_id
order by b.review_count desc
limit 10;
```

```
"Bacchanal Buffet"	"Las Vegas"	"3.78"	10129
"Mon Ami Gabi"	"Las Vegas"	"4.14"	9264
"Wicked Spoon"	"Las Vegas"	"3.68"	7383
"Hash House A Go Go"	"Las Vegas"	"3.92"	6751
"Gordon Ramsay BurGR"	"Las Vegas"	"3.89"	5494
"Earl of Sandwich"	"Las Vegas"	"4.28"	5232
"Yardbird Southern Table & Bar"	"Las Vegas"	"4.50"	4828
"Secret Pizza"	"Las Vegas"	"4.16"	4803
"The Buffet At Wynn"	"Las Vegas"	"3.64"	4803
"The Cosmopolitan of Las Vegas"	"Las Vegas"	"3.85"	4740
```

### Popular Given Names

```
select name
,count(*) as count
from yelp_academic_dataset.user
group by name
order by count(*) desc
limit 10;
```

```
"John"	"16931"
"Michael"	"16103"
"David"	"15977"
"Chris"	"14389"
"Mike"	"13299"
"Jennifer"	"12338"
"Jessica"	"11205"
"Sarah"	"10390"
"Michelle"	"10317"
"Lisa"	"9632"
```

For deeper analysis, the following takes given names shared by at least 1000 users and shows the counts of how many of those users joined in each year.

```
with user_names as (
	select name, count(*) as count
	from yelp_academic_dataset.user
	group by name
)
select name
,extract(year from yelping_since) as yelping_since_year
,count(*) as count
from yelp_academic_dataset.user
where name in (select name from user_names where count > 1000)
group by name, yelping_since_year;
```

The user sign-up counts should be normalized against the total sign-ups per year, since Yelp's popularity has varied.

```
select count(*) as count
,extract(year from yelping_since) as year
from yelp_academic_dataset.user
group by year;
```

### Popular Business Locations

```
select city, state, count(*) as count
from yelp_academic_dataset.business
group by city, state
order by count desc
limit 10;
```

```
"Las Vegas"	"NV"	"31623"
"Toronto"	"ON"	"20364"
"Phoenix"	"AZ"	"20170"
"Charlotte"	"NC"	"10417"
"Scottsdale"	"AZ"	"9341"
"Calgary"	"AB"	"8375"
"Pittsburgh"	"PA"	"7630"
"Montréal"	"QC"	"6979"
"Mesa"	"AZ"	"6577"
"Henderson"	"NV"	"5272"
```

A closer look at the city data shows that the city name field is full of errors (punctuation, partial addresses, whitespace), but we can still estimate the latitude and longitude of major urban centers.

```
select trim(lower(city)) as city
,state
,round(avg(latitude)::numeric, 3) as latitude
,round(avg(longitude)::numeric, 3) as longitude
,count(*) as business_count
,sum(review_count) as review_count
from yelp_academic_dataset.business
where city is not null
  and city != ''
group by city, state
order by city asc;
```
