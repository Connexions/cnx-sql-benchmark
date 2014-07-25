# -*- coding: utf-8 -*-
"""\
Build a SQL query cache of the cnx-archive search functionality.

Produce a built set of SQL statments that are then
used in timed executions.

The build queries are stored in a ``queries`` directory that
contains directories named by git revision based on what revision
cnx-archive was at when the build occured.

In order to collect the SQL, we monkey patch ``psycopg2`` to include
a custom ``Cursor`` class that will capture the query.

(Design note) The excution of the SQL statement is probably not necessary,
but having it execute makes sure the statement works, rather that have
it brake after the build. This is also, setup in such a way that it can run
against an empty database, as long as the database has the cnx-archive
schema initialized.

Usage:
  build.py <db_connection_string> <raw_query>... [--repo=<repo>]

Options:
  -h --help        This usage text.
  --repo=<repo>    The local directory of the cnx-archive repository clone
                   [default: ./cnx-archive]

"""
from __future__ import print_function

import os
import sys
import re
import json
import unicodedata

import git
import psycopg2
import psycopg2.extensions
from docopt import docopt

from cnxarchive.search import (
    DEFAULT_SEARCH_WEIGHTS,
    _build_search, Query,
    )

__version__ = "0.1"


def cursor_factory(capture_callback):
    """Factory for creating the cursor class with a ``capture_callback``
    that allows the factory executor to assign a callback function that
    will be invoked after a cursor has executed.
    """

    class CaptureCursor(psycopg2.extensions.cursor):

        def execute(self, sql, args=None):
            psycopg2.extensions.cursor.execute(self, sql, args)
            capture_callback(self)

    return CaptureCursor


def normalize_string(string):
    """Return a normalized string for the given string.
    This removes symbols, unicode characters and character case
    from the string to create a lowercase string
    with ASCII characters and hyphens (-) instead of spaces.
    """
    filtered_string = []
    if isinstance(string, str):
        string = unicode(string, 'utf-8')
    for i in unicodedata.normalize('NFKC', string):
        cat = unicodedata.category(i)[0]
        # filter out all the non letter and non number characters from the
        # input (L is letter and N is number)
        if cat in 'LN' or i in '-_':
            filtered_string.append(i)
        elif cat in 'Z':
            filtered_string.append(' ')
    return re.sub('\s+', '-', ''.join(filtered_string)).lower()


class WriteQueryCallback(object):
    """Write the Cursor query to the file."""

    def __init__(self, filepath, query):
        self.filepath = filepath
        self.query = query

    def __call__(self, cursor):
        with open(self.filepath, 'wa') as fb:
            fb.write("-- &query& = {}\n\n".format(self.query))
            fb.write(cursor.query)


def _produce_data_file(cache_dir, query_items):
    """Write the query items to file."""
    filepath = os.path.join(cache_dir, 'data-set.json')
    data_set = {
        'version': __version__,
        'data': query_items,
        }
    with open(filepath, 'w') as fb:
        json.dump(data_set, fb)


def main(args=None):
    """Cache the SQL statements"""
    parsed_args = docopt(__doc__, argv=args)
    cnxarchive_loc = parsed_args['--repo']
    db_conn_string = parsed_args['<db_connection_string>']
    queries = parsed_args['<raw_query>']

    # Acquire the cnx-archive revision as the cache directory name.
    repo = git.Repo(cnxarchive_loc)
    revision = repo.head.commit.hexsha

    # Set up the cache directory.
    cache_dir = os.path.join(os.getcwd(), 'cache', revision)
    try:
        os.makedirs(cache_dir)
    except OSError as exc:
        print("Problem creating cache directory '{}'.".format(cache_dir),
              file=sys.stderr)
        raise

    # Set up the weight values for the search query function.
    weights = {}
    for default in [(key, 0) for key in DEFAULT_SEARCH_WEIGHTS]:
        weights.setdefault(*default)

    # Build a list of queryies with normalized versions of the query
    # and the filename where the cached query will live.
    query_items = []
    for query in queries:
        normalized_query = normalize_string(query)
        filename = "{}.sql".format(normalized_query)
        query_items.append((query, normalized_query, filename,))

    # Build the statements one at a time.
    with psycopg2.connect(db_conn_string) as conn:
        for query, normalized_query, filename in query_items:
            # Give the built statement a place to live.
            filepath = os.path.join(cache_dir, filename)

            # Call the search logic to build the query.
            query_obj = Query.from_raw_query(query)
            statement, arguments = _build_search(query_obj, weights)

            # Make the callback and call the query.
            capture_query = WriteQueryCallback(filepath, query)
            factory = cursor_factory(capture_query)
            with conn.cursor(cursor_factory=factory) as cursor:
                cursor.execute(statement, arguments)

    # Place a data file in the directory in order to reverse the process
    # and/or create a new benchmark point from the data set.
    _produce_data_file(cache_dir, query_items)

    return 0


if __name__ == '__main__':
    sys.exit(main())
