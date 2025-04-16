#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Update project version
#
# arg1: pom.xml
# arg2: new version number
# arg3: optional suffix for project name
#
import sys
import xml.etree.ElementTree as ET
from xml.sax.saxutils import unescape

pom_file = sys.argv[1]
version = sys.argv[2]
suffix = None
if len(sys.argv) > 3:
    suffix = sys.argv[3]

def remove_namespace(doc, namespace):
    """
    Remove namespace in the passed document in place.
    """
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.getiterator():
        if type(elem.tag) is str and elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]

class PCParser(ET.XMLTreeBuilder):
    """
    A parser including the comments
    """
    def __init__(self):
        ET.XMLTreeBuilder.__init__(self)
        # assumes ElementTree 1.2.X
        self._parser.CommentHandler = self.handle_comment

    def handle_comment(self, data):
        self._target.start(ET.Comment, {})
        self._target.data(unescape(data.strip())) # strip spaces and unescape
        self._target.end(ET.Comment)

tree = ET.parse(pom_file, parser=PCParser())
root = tree.getroot()
for e1 in root:
    if type(e1.tag) is str and e1.tag.endswith('version'):
        e1.text = version
    if suffix and type(e1.tag) is str and e1.tag.endswith('artifactId'):
        e1.text = e1.text + '-' + suffix

remove_namespace(root, u'http://maven.apache.org/POM/4.0.0')

tree.write(sys.stdout)