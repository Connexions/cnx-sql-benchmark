# -*- coding: utf-8 -*-
"""\
Produce a benchmark measurement for a set of cached queries indicated by
a git revision or by pointing directly to the data-set file.

Usage:
  benchmark.py <db_connection_string>
    (--data-set=<filepath> | --git-revision=<revision>)

Options:
  -h --help                  This usage text
  --data-set=<filepath>      The data-set file created by build.py
                             [default: ./data-set.json]
  --git-revision=<revision>  The git revision of cnx-archive

"""
from __future__ import print_function

import os
import sys
import csv
import json
import time

import psycopg2
from docopt import docopt


class Clock(object):
    """Clock used to time an execution"""

    def __init__(self):
        self._start = None
        self._stop = None

    def start(self):
        self._start = time.time()

    def stop(self):
        self._stop = time.time()

    @property
    def time(self):
        if self._start is None or self._stop is None:
            return None
        return self._stop - self._start


class Benchmark(object):
    """Benchmarking context manager"""

    def __init__(self, clock=None):
        if clock is None:
            clock = Clock()
        self.clock = clock
        self.interval = None

    def __enter__(self):
        self.clock.start()
        return self

    def __exit__(self, ty, val, tb):
        self.clock.stop()
        self.interval = self.clock.time


def benchmark_query(cursor, query):
    """Produce benchmark stats for the given query."""
    with Benchmark() as bm:
        cursor.execute(query)
        cursor.fetchall()
    return bm.interval


def main(args=None):
    parsed_args = docopt(__doc__, argv=args)
    db_conn_string = parsed_args['<db_connection_string>']
    git_revision = parsed_args['--git-revision']
    data_set_filepath = parsed_args['--data-set']
    if git_revision is None:
        query_loc = os.path.dirname(data_set_filepath)
    else:
        query_loc = os.path.join('cache', git_revision)
        data_set_filepath = os.path.join(query_loc, 'data-set.json')
    if not os.path.exists(data_set_filepath):
        raise RuntimeError("Could not find the data-set file at '{}'"
                           .format(data_set_filepath))

    # Discover the queries to benchmark.
    with open(data_set_filepath, 'r') as fb:
        data_set = json.load(fb)
        query_items = data_set['data']
    items = [dict(zip(['query', 'normalized', 'filename'], x))
             for x in query_items]

    # Set up the connection and begin benchmarking.
    with psycopg2.connect(db_conn_string) as db_conn:
        with db_conn.cursor() as cursor:
            # Not sure this is necessary, but can't hurt.
            cursor.execute("SELECT 1")
            cursor.fetchone()
            marks = []
            for i, item in enumerate(items):
                filepath = os.path.join(query_loc, item['filename'])
                with open(filepath, 'r') as fb:
                    query = fb.read()
                interval = benchmark_query(cursor, query)
                marks.insert(i, interval)
                print("Ran '{}' in {}ms".format(item['normalized'],
                                                round(interval * 1000)),
                      file=sys.stderr)

    # Write the results to CSV file.
    with open('marks.csv', 'w') as fb:
        csv_results = csv.writer(fb)
        for i, item in enumerate(items):
            row = [item['query'], item['filename'], marks[i]]
            csv_results.writerow(row)            

    return 0

if __name__ == '__main__':
    sys.exit(main())
