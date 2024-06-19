#!/bin/env python

import io
import json
import re
import resource
import sqlite3
import sys
import traceback
from collections import namedtuple

# since building the graph tree could be very recursive raise the limit from 1k to 8k and the program stack size
resource.setrlimit(resource.RLIMIT_STACK, [0x4000000, resource.RLIM_INFINITY])
sys.setrecursionlimit(0x20000)


def get_conn(db_name):
    try:
        return sqlite3.connect(db_name)
    except sqlite3.Error as e:
        print(e)


def namedtuple_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)


def get_submissions(conn):
    conn.row_factory = namedtuple_factory
    cursor = conn.cursor()
    try:
        for row in cursor.execute("SELECT * FROM submissions"):
            # print(row)
            yield row
    except sqlite3.OperationalError:
        print(traceback.format_exc())


def get_comments_flattened(conn, link_id):
    # print(f"\n\n*** {link_id=}")
    conn.row_factory = namedtuple_factory

    cursor = conn.cursor()
    try:
        res = cursor.execute(
            "SELECT * FROM comments WHERE link_id = ?", ("t3_" + link_id,)
        )
    except sqlite3.OperationalError:
        print(traceback.format_exc())
        return ""

    comments = res.fetchall()
    pairs = []
    lookup = {}
    for comment in comments:
        parent_id = re.sub(r"t\d_", "", comment.parent_id)
        pairs += [(parent_id, comment.id)]
        lookup[comment.id] = comment.body
    # print(pairs)

    # adapted from: https://stackoverflow.com/a/45461474/9201239
    # Build a directed graph and a list of all names that have no parent
    graph = {name: set() for tup in pairs for name in tup}
    has_parent = {name: False for tup in pairs for name in tup}
    for parent, child in pairs:
        graph[parent].add(child)
        has_parent[child] = True

    # All names that have absolutely no parent:
    roots = [name for name, parents in has_parent.items() if not parents]

    # # traversal of the graph (doesn't care about duplicates and cycles)
    # def traverse(hierarchy, graph, names):
    #     for name in names:
    #         hierarchy[name] = traverse({}, graph, graph[name])
    #     return hierarchy
    # from pprint import pprint
    # pprint(traverse({}, graph, roots))

    # build a flatten body of comments while traversing the graph
    # include |-style levels like in email to indicate multiple levels of replies
    # skip [removed] comments (and possibly any replies to those? body=="[removed]")
    body = []

    def traverse(graph, names, body, level):
        level += 1

        # uncomment if you want email-like replies to be visible with | prefices
        # prefix = "|"*level + " " if level else ""
        prefix = ""
        for name in names:
            # print(name)
            if name in lookup and lookup[name] != "[removed]":
                body += [prefix + lookup[name]]
            traverse(graph, graph[name], body, level)
        return

    traverse(graph, roots, body, -1)

    comments_flat = "\n".join(body)
    # print("\n----------------------------\n")
    # print(comments_flat)
    # print("\n----------------------------\n")

    return comments_flat


# test with
# https://www.reddit.com/r/investingforbeginners/comments/h9ckeh/stock_simulator/

db_names = sys.argv[1:]

for db_name in db_names:
    f = db_name.replace(".db", ".jsonl")

    print(f"{db_name} => {f}")

    conn = get_conn(db_name)

    fh = io.open(f, "w", encoding="utf-8")
    c = 0
    for submission in get_submissions(conn):
        c += 1
        if c % 1000 == 0:
            print(f"\rRow {c}", end="")
        # print(f"\n\n--- {submission.num_comments} {submission.id} {submission.title}")
        # print(submission)
        # print(submission.score)

        data = [submission.title, submission.selftext, ""]
        # print(submission.title)
        # print(submission.num_comments)
        # print(submission.selftext)

        # don't bother with submissions w/ barely any body and less than 2 comments?
        # there are some big submissions w/o comments
        if int(submission.num_comments) < 2 and len(submission.selftext) < 150:
            continue

        if int(submission.num_comments) != 0:
            data += [get_comments_flattened(conn, submission.id)]

        data_flat = "\n".join(data)

        json.dump(dict(text=data_flat), fh, sort_keys=True, ensure_ascii=False)
        fh.write("\n")

        # sys.exit()

    print(f"\rRow {c}")

print("Done")
