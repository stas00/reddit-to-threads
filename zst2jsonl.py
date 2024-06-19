#!/bin/env python

# adapted from https://github.com/ArthurHeitmann/arctic_shift/
#
# This script converts .zst files from https://github.com/ArthurHeitmann/arctic_shift/ Reddit dumps into jsonl dumps
#
# Example: if you downloaded this sub dump as careerguidance_comments.zst and careerguidance_submissions.zst
#
# ./zst2jsonl.py careerguidance_*.zst

import io
import json
import os
import sys
from typing import Iterable

from fileStreams import getFileJsonStream

version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")

fileOrFolderPath = sys.argv[-1]
recursive = False


def processFile(path: str):
    jsonStream = getFileJsonStream(path)
    if path.endswith(".jsonl"):
        print(f"skip {path}")
        return
    path_out = path.replace("zst", "jsonl")
    assert (
        path != path_out
    ), f"broke the assumption of input file's ext to be .zst {path=} {path_out=}"
    if jsonStream is None:
        print(f"Skipping unknown file {path}")
        return

    with io.open(path_out, "w", encoding="utf-8") as fh:
        i = 0
        for i, (lineLength, row) in enumerate(jsonStream):
            if i % 10_000 == 0:
                print(f"\rRow {i}", end="")
            json.dump(row, fh, sort_keys=True, ensure_ascii=False)
            fh.write("\n")
        print(f"\rRow {i+1}")


def processFolder(path: str):
    fileIterator: Iterable[str]
    if recursive:

        def recursiveFileIterator():
            for root, dirs, files in os.walk(path):
                for file in files:
                    yield os.path.join(root, file)

        fileIterator = recursiveFileIterator()
    else:
        fileIterator = os.listdir(path)
        fileIterator = (os.path.join(path, file) for file in fileIterator)

    for i, file in enumerate(fileIterator):
        print(f"Processing file {i+1: 3} {file}")
        processFile(file)


def main():
    if os.path.isdir(fileOrFolderPath):
        processFolder(fileOrFolderPath)
    else:
        processFile(fileOrFolderPath)

    print("Done :>")


if __name__ == "__main__":
    main()
