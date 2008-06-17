import xml.dom.minidom
from gip_common import fileRead
from xml.sax import make_parser
from xml.sax.handler import ContentHandler, feature_external_ges

def getText(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc

def getDom(source, sourcetype="string"):
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


