
"""
Convenience tools for dealing with XML in the GIP.

"""
import os
import xml.dom.minidom
from xml.sax import make_parser
from xml.sax.handler import feature_external_ges

class XMLDom:
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

    def addXSL(self, stylesheet):
        data = 'type="text/xsl" href="%s"' % stylesheet
        self.doc.createProcessingInstruction("xml-stylesheet", data)

    def loadXML(self, source):
        """
        source is a string containing the xml to be parsed
        """
        self.doc = xml.dom.minidom.parseString(source)
        
    def getDom(self):
        return self.doc
    
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
    
    def toXML(self, pretty=False):
        if pretty:
            xml_contents = self.doc.toprettyxml()
        else:
            xml_contents = self.doc.toxml()
        
        return xml_contents

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
