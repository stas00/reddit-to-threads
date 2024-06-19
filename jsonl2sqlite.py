#!/bin/env python

import io
import json
import re
import sqlite3
import sys

files = sys.argv[1:]


def execute(db_name, statement, data=()):
    # create a database connection
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(statement, data)

            conn.commit()
    except sqlite3.Error as e:
        print(e)


def create_submissions_table(db_name):
    statement = """CREATE TABLE IF NOT EXISTS submissions (
                xid INTEGER PRIMARY KEY,
                id TEXT NOT NULL,
                title TEXT NOT NULL,
                selftext TEXT NOT NULL,
                num_comments TEXT NOT NULL,
                score TEXT NOT NULL,
                created_utc TEXT NOT NULL
        );"""
    execute(db_name, statement)


def create_comments_table(db_name):
    statement = """CREATE TABLE IF NOT EXISTS comments (
                xid INTEGER PRIMARY KEY,
                id TEXT NOT NULL,
                link_id TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                body TEXT NOT NULL,
                score TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                FOREIGN KEY (link_id) REFERENCES submissions (id)
        );"""
    execute(db_name, statement)


def insert_submission(db_name, data):
    statement = """INSERT INTO submissions(id, title, selftext, num_comments, score, created_utc)
             VALUES(?,?,?,?,?,?) """
    # print(data)
    execute(db_name, statement, data)


def insert_comment(db_name, data):
    statement = """INSERT INTO comments(id, link_id, parent_id, body, score, created_utc)
             VALUES(?,?,?,?,?,?) """
    # print(data)
    execute(db_name, statement, data)


submission_keys = ["id", "title", "selftext", "num_comments", "score", "created_utc"]
comment_keys = ["id", "link_id", "parent_id", "body", "score", "created_utc"]


def get_submission(d):
    return list(map(d.get, submission_keys))


def get_comment(d):
    return list(map(d.get, comment_keys))


for f in files:
    db_name = re.sub(r"_(comments|submissions).jsonl", ".db", f)

    is_submission = "_submissions" in f

    if is_submission:
        create_submissions_table(db_name)
    else:
        create_comments_table(db_name)

    print(f"Input: {f}")
    with io.open(f, "r", encoding="utf-8") as fh:
        c = 0
        for ln in fh:
            c += 1
            if c % 1000 == 0:
                print(f"\rRow {c}", end="")
            e = json.loads(ln)

            if is_submission:
                insert_submission(db_name, get_submission(e))
            else:
                insert_comment(db_name, get_comment(e))
        print(f"\rRow {c}")

print("Done")
