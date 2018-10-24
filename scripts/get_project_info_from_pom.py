#!/usr/bin/env python

import sys
import os
import xml.etree.ElementTree as ET

VALID_TAGS= frozenset([
    'version',
    'artifactId',
    'groupId'])

if len(sys.argv) != 3:
    print("Usage: {0} <pom.xml> <tag>".format(sys.argv[0]))
    sys.exit(1)
pom_file = sys.argv[1]
tag = sys.argv[2]

if not os.path.exists(pom_file):
    print("pom.xml doesn't exist: {0}".format(pom_file))
    sys.exit(1)

if tag not in VALID_TAGS:
    print("Invalid tag: {0}. It should be either one of them {1}".format(
        tag,
        ','.join(VALID_TAGS)))
    sys.exit(1)

tree = ET.parse(pom_file)
root = tree.getroot()
for e1 in root:
    if type(e1.tag) is str and e1.tag.endswith(tag):
        print(e1.text)
        break