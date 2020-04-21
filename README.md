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
  * `psql -h localhost -p 5432 -U postgres -f pg_yelp_schema.sql.sql`
* Set the database credentials.
  * `echo "dbname='postgres' user='postgres' host='localhost' password='adminpass'" >connection_string.txt`
* Run the import script.
  * `python3 pg_yelp_import.py`
