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
