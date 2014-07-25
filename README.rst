Benchmarking Utility
====================

Contains two scripts: ``build.py`` and ``benchmark.py``

Installation
------------

This requires cnx-archive be installed.

It also requires these third-party dependencies::

    pip install docopt gitpython

Usage
-----

Build a set of cached queries using one of the following commands::

    python build.py "dbname=cnxarchive-testing user=cnxarchive" \
    --repo=../cnx-archive/ "carpentry"

Or::

    python build.py "dbname=cnxarchive-testing user=cnxarchive" \
    --repo=../cnx-archive/ \
    --from-data-set=./cache/ff781b8ab034866eb0b36adc10e76abc75144601/data-set.json

After the data-set has been build into a cache of SQL statements
you can run the benchmark tool over the data-set to observe the
time it takes to get back results from the raw SQL statements.

::

    python benchmark.py "dbname=pumazi user=pumazi password=pumazi" --data-set=cache/ff781b8ab034866eb0b36adc10e76abc75144601/data-set.json

This will print the times out in the console as well as produce
a CSV file of the results for later analysis.
