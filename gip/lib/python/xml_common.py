import xml.dom.minidom
from gip_common import fileRead

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
