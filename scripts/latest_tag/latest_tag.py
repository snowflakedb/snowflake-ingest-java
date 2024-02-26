#!/usr/bin/env python3
# coding: utf-8

import os
import re
import subprocess
import sys

import snowflake.connector

STARTING_AFTER_MAJOR = 2
STARTING_AFTER_MINOR = 0
STARTING_AFTER_PATCH = 4

def tags_from_git():
    result = subprocess.run(["git", "tag"], check=True, text=True)
    tags = result.stdout.splitlines()
    return tags

def classify_tags(tags):
    version_number = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
    tags_without_version_number = []
    old_tags = []
    invalid_tags = []
    valid_tags = []
    for i in range(len(tags)):
        tag = tags[i]
        match = version_number.search(tag)
        if not match:
            tags_without_version_number.append(tag)
            continue
        version_number = match.group()
        split = version_number.split(".", 3)
        major = int(split[0])
        minor = int(split[1])
        patch = int(split[2])
        if major < STARTING_AFTER_MAJOR:
            old_tags.append(tag)
            continue
        if major == STARTING_AFTER_MAJOR and minor < STARTING_AFTER_MINOR:
            old_tags.append(tag)
            continue
        if major == STARTING_AFTER_MAJOR and minor == STARTING_AFTER_MINOR and patch < STARTING_AFTER_PATCH:
            old_tags.append(tag)
            continue
        if not tag.startswith('v'):
            invalid_tags.append(tag)
            continue
        if not match.start() == 1:
            invalid_tags.append(tag)
            continue
        if not match.end() == len(tag):
            invalid_tags.append(tag)
            continue
        valid_tags.append({"tag": tag, "version_number": (major, minor, patch)})
    return valid_tags, old_tags, invalid_tags, tags_without_version_number

def latest(valid_tags):
    valid_tags.sort(key=lambda tag: tag["version_number"], reverse=True)
    return valid_tags[0]

def main():
    tags = tags_from_git()
    # old_tags, invalid_tags and tags_without_version_number are ignored
    # we could issue a warning for invalid_tags but decided that they are not probable
    valid_tags, old_tags, invalid_tags, tags_without_version_number = classify_tags(tags)
    if (len(valid_tags) == 0):
        sys.exit(1)
    latest_tag = latest(valid_tags)
    print(latest_tag["tag"])

if __name__ == "__main__":
    main()