# pg-yelp-import

Scripts to import Yelp Academic Dataset into Postgres.

Working with 10 GB of data as a handful of newline delimited files containing json docs can be unwieldy. Having the data as Postgres tables makes it easier to filter, summarize, and further transform the data set.

## Caveats

This project is for my own personal learning purposes only. Do not use this in a professional or production context. Be skeptical of the code.

The structure of the Yelp Dataset is likely to change over time and this project is unlikely to be maintained. Do not expect anything to work.

## Tech Stack

* Python 3 with psycopg2
* PostgreSQL 11

## Setup

* Install the required Python libs.
  * `pipenv install`
* Create the Postgres tables.
  * `psql -h localhost -p 5432 -U postgres -f pg_yelp_schema.sql.sql`
* Set the database credentials.
  * `echo "dbname='postgres' user='postgres' host='localhost' password='adminpass'" >connection_string.txt`
  * TODO: example of this
* Run the import script.
  * `python3 pg_yelp_import.py`
