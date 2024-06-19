#!/bin/env python

# adapted from https://github.com/ArthurHeitmann/arctic_shift/
#
# This script converts .zst files from https://github.com/ArthurHeitmann/arctic_shift/ Reddit dumps into sqlite3 database merging comments and submissions per sub
#
# Example: if you downloaded this sub dump as careerguidance_comments.zst and careerguidance_submissions.zst
#
# ./zst2sqlite.py careerguidance_*.zst


import re
import sqlite3
import sys

from fileStreams import getFileJsonStream

version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")


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


files = sys.argv[1:]

for f in files:
    jsonStream = getFileJsonStream(f)
    if f.endswith(".jsonl") or f.endswith(".db"):
        print(f"skip {f}")
        continue

    db_name = re.sub(r"_(comments|submissions).zst", ".db", f)
    print(f"{f} => {db_name}")

    is_submission = "_submissions" in f

    if is_submission:
        create_submissions_table(db_name)
    else:
        create_comments_table(db_name)

    # path_out = path.replace("zst", "jsonl")
    # assert path != path_out, f"broke the assumption of input file's ext to be .zst {path=} {path_out=}"

    if jsonStream is None:
        print(f"Skipping unknown file {f}")
        continue

    i = 0
    for i, (line_length, row) in enumerate(jsonStream):
        if i % 1000 == 0:
            print(f"\rRow {i}", end="")

        if is_submission:
            insert_submission(db_name, get_submission(row))
        else:
            insert_comment(db_name, get_comment(row))

        # json.dump(row, fh, sort_keys=True, ensure_ascii=False)
        # fh.write("\n")
    print(f"\rRow {i+1}")

print("Done")
