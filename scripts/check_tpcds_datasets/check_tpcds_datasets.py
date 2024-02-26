import os
import re

import snowflake.connector

STARTING_AFTER_MAJOR = 2
STARTING_AFTER_MINOR = 0
STARTING_AFTER_PATCH = 4

def tags_from_env():
    tags_env_var = os.getenv("TAGS")
    tags = tags_env_var.splitlines()
    return tags

def classify_tags(tags):
    version_number = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
    tags_without_version_number = []
    old_tags = []
    invalid_tags = []
    valid_tags = []
    version_numbers = []
    for i in range(len(tags)):
        tag = tags[i]
        match = version_number.search(tag)
        if not match:
            tags_without_version_number.append(tag)
            continue
        version_number = match.group()
        split = version_number.split(".", 3)
        major = split[0]
        minor = split[1]
        patch = split[2]
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
        valid_tags.append(tag)
        version_numbers.append((major, minor, patch))
    return valid_tags, old_tags, invalid_tags, tags_without_version_number, version_numbers

# returns the authenticated snowflake connection
def init_snowflake():
    snowflake_config = {
        "user": os.getenv("APP_JENKINS_USER"),
        "password": os.getenv("APP_JENKINS_PASSWORD"),
        "host": os.getenv("HOST"),
        "account": os.getenv("ACCOUNT"),
        "protocol": 'https',
        "port": int(os.getenv("PORT")),
        "role": "SYSADMIN"
    }

    return snowflake.connector.connect(**snowflake_config)

def is_complete(tag, con):
    check_complete_sql = CHECK_COMPLETE.format(os.getenv("DATABASE_PREFIX") + "_" + tag, os.getenv("SCHEMA"))
    cur = con.cursor()
    try:
        cur.execute(check_complete_sql)
        if cur.rowcount is 1:
            return True
    except snowflake.connector.errors.ProgrammingError:
        pass
    finally:
        cur.close()
    return False

def check_datasets_exist(tags):
    connection = init_snowflake()
    dataset_exists = []
    try:
        dataset_exists = []
    finally:
        connection.close()
    return dataset_exists
def main():
    tags = tags_from_env()
    valid_tags, old_tags, invalid_tags, tags_without_version_number, version_numbers = classify_tags(tags)

if __name__ == "__main__":
    main()