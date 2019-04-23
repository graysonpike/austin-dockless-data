# Helper functions to write to Sqlite databases

import sqlite3
import os


CONN = None
VALUES_SYNTAX = None
TABLENAME = None

def init_db(filename, schema_filename, tablename, num_cols):
    global CONN
    global TABLENAME
    if os.path.exists(filename):
        os.remove(filename)
    CONN = sqlite3.connect(filename)
    TABLENAME = tablename
    c = CONN.cursor()

    # Ensure results table exists
    if c.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s';" % (TABLENAME)).fetchone()[0] == 0:
        schema = open(schema_filename).read()
        c.executescript(schema)

    # Create a string of question marks inside parentheses for SQL syntax
    global VALUES_SYNTAX
    VALUES_SYNTAX = '('
    for i in range(num_cols):
        if i == num_cols - 1:
            VALUES_SYNTAX += '?)'
        else:
            VALUES_SYNTAX += '?,'


def append_row(row):
    c = CONN.cursor()
    c.execute("INSERT INTO %s VALUES %s" % (TABLENAME, VALUES_SYNTAX), row)


def commit_changes():
    CONN.commit()