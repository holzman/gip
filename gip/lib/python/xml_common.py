
"""
Convenience tools for dealing with XML in the GIP.

"""

import xml.dom.minidom
from gip_common import fileRead
from xml.sax import make_parser
from xml.sax.handler import feature_external_ges

def getText(nodelist):
    """
    Given a nodelist, iterate through it and concatenate the contents of any
    text nodes which are found.
    
    @param nodelist: List of XML nodes
    @return: The concatenated text of any present TEXT_NODEs.
    """
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc

def getDom(source, sourcetype="string"):
    """
    From a source, parse it as XML and return a DOM object.
    
    @param source: The source object to parse for the DOM
    @keyword sourcetype: The type of the source object; either "string" for the contents
        contained in the source string or "file" if source is a filename.
    @return: A DOM object parsed from the source.
    """
    dom = None
    if sourcetype == "string":
        dom = xml.dom.minidom.parseString(source)
    elif sourcetype == "file":
        dom = xml.dom.minidom.parseString(fileRead(source))

    return dom

def parseXmlSax(fp, handler):
    """
    Parse XML using the SAX parser.

    Create a SAX parser with the content handler B{handler}, then parse the
    contents of B{fp} with it.

    @param fp: A file-like object of the Condor XML data
    @param handler: An object which will be our content handler.
    @type handler: xml.sax.handler.ContentHandler
    @returns: None
    """
    parser = make_parser()
    parser.setContentHandler(handler)
    parser.setFeature(feature_external_ges, False)
    parser.parse(fp)


