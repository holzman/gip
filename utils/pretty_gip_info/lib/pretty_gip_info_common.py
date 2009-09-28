'''
Created on Sep 1, 2009

@author: tiradani
'''

import os
import types
import tempfile
import cStringIO
import xml.dom.minidom
from lib.gip_ldap import read_ldap

def safeGet(dictionary, key, default=""):
    try:
        val = ", ".join(dictionary[key])
    except KeyError:
        val = default
    return val

def isStringType(obj):
    # note: doing it this way for python 2.2 compatibility
    stringType = False
    obj_type = type(obj)
    for t in types.StringTypes:
        if obj_type == t:
            stringType = True
            break
    return stringType

def write_file(path, contents, mode="w"):
    f = open(path, mode)
    f.write(contents)
    f.close()

def read_file(path):
    return open(path).read()

def getTempFilename():
    try:
        conffile = tempfile.NamedTemporaryFile()
        conffile = conffile.name
    except:
        conffile = tempfile.mktemp()
    return conffile

class GIP_XML_DOM:
    def __init__(self):
        self.doc = xml.dom.minidom.Document()

    def addChild(self, leaf, child_name, text=""):
        child = self.doc.createElement(child_name)
        leaf.appendChild(child)
        text = str(text)
        if len(text) > 0:
            txtNode = self.doc.createTextNode(text)
            child.appendChild(txtNode)
        return child

    def transform(self, html_loc, xsl_loc, xml_loc):
        transform_cmd = "xsltproc -o %s %s %s" % (html_loc, xsl_loc, xml_loc)
        os.popen(transform_cmd)

    def getDom(self, source, sourcetype="string"):
        dom = None
        if sourcetype == "string":
            self.doc = xml.dom.minidom.parseString(source)
        elif sourcetype == "file":
            self.doc = xml.dom.minidom.parseString(read_file(source))
    
        return dom
    def getText(self, nodelist):
        rc = ""
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc = rc + node.data
            if node.childNodes:
                for child in node.childNodes:
                    if child.nodeType == node.CDATA_SECTION_NODE:
                        rc = rc + child.data
        return rc
    
    def toXML(self, filename, pretty=False):
        if pretty:
            contents = self.doc.toprettyxml()
        else:
            contents = self.doc.toxml()
        write_file(filename, contents)

class LdifToXML(GIP_XML_DOM):
    def __init__(self, source="gip", source_path=""):
        GIP_XML_DOM.__init__(self)
        self.source_path = source_path
        self.entries = None
        self.getEntries(source.lower())
        
    def getEntries(self, source):
        if source == "gip":
            self.source_path = os.path.expandvars("$GIP_LOCATION/bin/gip_info")
            fd = os.popen(self.source_path)
        elif source == "file":
            fd = open(self.source_path)
        elif source == "string":
            fd = cStringIO.StringIO()
        self.entries = read_ldap(fd, multi=True)

    def parseLdif(self):
        pass

    def MapLdif(self, stanza_map, item_name, source):
        value = ""
        item = stanza_map[item_name]
        if hasattr(item, "__iter__"):
            # we are a dict containing formatting
            format = item["format"]
            name_list = item["names"].split(",")
            if format == "list":
                for name in name_list:
                    value += safeGet(source, name, default="")
            else:
                values = []
                for name in name_list:
                    values.append(safeGet(source, name))
                value = format % tuple(values)
        elif isStringType(item):
            value = safeGet(source, item)
        return value
