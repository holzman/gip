#!/usr/bin/env python

import os
import sys
from xml.dom.minidom import Document

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_ldap import read_ldap
from gip_common import config, cp_get, getTempFilename
from gip_common import fileOverWrite as write_file

def runCommand(cmd):
    return os.popen(cmd)

def getText(node):
    rc = ""
    nodelist = node.childNodes
    for child in nodelist:
        if child.nodeType == child.TEXT_NODE:
            rc = rc + child.data
    return rc

def setText(dom, node, newText):
    newText = str(newText)
    nodelist = node.childNodes
    for child in nodelist:
        if child.nodeType == child.TEXT_NODE:
            node.removeChild(child)
    txtNode = dom.createTextNode(newText)
    node.appendChild(txtNode)

def addChild(dom, leaf, child_name, text=""):
    child = dom.createElement(child_name)
    leaf.appendChild(child)
    text = str(text)
    if len(text) > 0:
        txtNode = dom.createTextNode(text)
        child.appendChild(txtNode)

    return child

def writeXML(dom, filename, pretty=False):
    if pretty:
        contents = dom.toprettyxml()
    else:
        contents = dom.toxml()
    write_file(filename, contents)

class SiteInfoToXml:
    def __init__(self):
        cp = config()
        self.xml_file = getTempFilename()
        self.xsl_file = os.path.expandvars(cp_get(cp, "gip", "pretty_gip_xsl",
            "$GIP_LOCATION/templates/pretty_gip_info.xsl"))
        self.html_file = os.path.expandvars(cp_get(cp, "gip", "pretty_gip_html",
            "$VDT_LOCATION/apache/htdocs/pretty_gip_info.html"))
        self.doc = Document()
        self.entries = None

    def main(self):
        self.getEntries()
        self.parseLdif()
        self.transform()

    def transform(self):
        writeXML(self.doc, self.xml_file, pretty=True)
        transform_cmd = "xsltproc -o %s %s %s"
#        runCommand(transform_cmd % (self.html_file,self.xsl_file,self.xml_file))
#        runCommand("rm -rf %s" % self.xml_file)

    def getEntries(self):
        path = os.path.expandvars("$GIP_LOCATION/bin/gip_info")
        fd = os.popen(path)
        self.entries = read_ldap(fd, multi=True)

    def getLdifValue(self, entry_value):
        try:
            val = ", ".join(entry_value)
        except:
            val = "EXCEPTION"
        return val

    def findEntry(self, node, elm_name, id=""):
        rtnNode = None
        nodes = node.getElementsByTagName(elm_name)
        for node in nodes:
            try:
                if id == "" or node.attributes["id"].value == id:
                    rtnNode = node
                    break
            except: # element did not have an attribute called "id"
                continue
        return rtnNode

    def convertEntry(self, entry, parentNode):
        keys = entry.glue.keys()
        for key in keys:
            elm = self.findEntry(parentNode, key)
            elm_text = self.getLdifValue(entry.glue[key])
            if elm is None:
                addChild(self.doc, parentNode, key, text=elm_text)
            else:
                setText(self.doc, elm, elm_text)

    def updateItem(self, dom, parentNode, childName, value):
        elm = self.findEntry(parentNode, childName)
        if elm is None:
            addChild(self.doc, parentNode, childName, text=value)
        else:
            setText(self.doc, elm, value)

    def parseLdif(self):
        xml = addChild(self.doc, self.doc, "xml")
        for entry in self.entries:
            dn = list(entry.dn)
            stanza_type = dn[0].split("=")[0]

            if stanza_type == "GlueCEUniqueID":
                cluster_id = entry.glue['ForeignKey'][0].split("=")[1]
                cluster = self.findEntry(xml, "cluster", cluster_id)
                if cluster is None:
                    cluster = addChild(self.doc, xml, "cluster")
                    cluster.attributes["id"] = cluster_id
                ce = addChild(self.doc, cluster, "clusterCE")
                self.convertEntry(entry, ce)
                
            elif stanza_type == "GlueSEUniqueID":
                se_id = self.getLdifValue(entry.glue['SEName'])
                se = self.findEntry(xml, "se", se_id)
                if se is None:
                    se = addChild(self.doc, xml, "se")
                    se.attributes["id"] = se_id
                self.convertEntry(entry, se)

            elif stanza_type == "GlueSALocalID":
                se_id = entry.glue['ChunkKey'][0].split("=")[1]
                se = self.findEntry(xml, "se", se_id)
                if se is None:
                    se = addChild(self.doc, xml, "se")
                    se.attributes["id"] = se_id
                pool = addChild(self.doc, se, "pool")
                self.convertEntry(entry, pool)

            elif stanza_type == "GlueSEAccessProtocolLocalID":
                se_id = entry.glue['ChunkKey'][0].split("=")[1]
                se = self.findEntry(xml, "se", se_id)
                if se is None:
                    se = addChild(self.doc, xml, "se")
                    se.attributes["id"] = se_id

                nodeList = None
                door_id = entry.glue['SEAccessProtocolType'][0]
                door = self.findEntry(se, "door", door_id)
                if door is None:
                    door = addChild(self.doc, se, "door")
                    door.attributes["id"] = door_id
                    self.convertEntry(entry, door)
                    nodeList = addChild(self.doc, door, "nodeList")
                else:
                    nodeList = findEntry(self.doc, door, "nodeList")

                node = entry.glue['SEAccessProtocolEndpoint'][0].split(":")[1][2:]
                nodes = getText(nodeList) + str(node) + " "
                setText(self.doc, nodeList, nodes)

            elif stanza_type == "GlueServiceUniqueID":
                # Add Service
                service = addChild(self.doc, xml, "service")
                self.convertEntry(entry, service)
                
            elif stanza_type == "GlueSEControlProtocolLocalID":
                se_id = entry.glue['ChunkKey'][0].split("=")[1]
                se = self.findEntry(xml, "se", se_id)
                if se is None:
                    se = addChild(self.doc, xml, "se")
                    se.attributes["id"] = se_id

                # Add Control Protocol to the SE
                secp = addChild(self.doc, se, "seControlProtocol")
                self.convertEntry(entry, secp)
                
            elif stanza_type == "GlueClusterUniqueID":
                cluster_id = self.getLdifValue(entry.glue['ClusterName'])
                cluster = self.findEntry(xml, "cluster", cluster_id)
                if cluster is None:
                    cluster = addChild(self.doc, xml, "cluster")
                    cluster.attributes["id"] = cluster_id
                # Update Cluster
                self.convertEntry(entry, cluster)
                
            elif stanza_type == "GlueSubClusterUniqueID":
                cluster_id = entry.glue['ChunkKey'][0].split("=")[1]
                cluster = self.findEntry(self.doc, "cluster", cluster_id)
                if cluster is None:
                    cluster = addChild(self.doc, xml, "cluster")
                    cluster.attributes["id"] = cluster_id
                subcluster = addChild(self.doc, cluster, "subcluster")
                self.convertEntry(entry, subcluster)

            elif stanza_type == "GlueSiteUniqueID":
                # Add site info
                site = self.findEntry(self.doc, "site")
                if site is None:
                    site = addChild(self.doc, xml, "site")
                self.convertEntry(entry, site)

            elif stanza_type == "GlueLocationLocalID":
                site = self.findEntry(self.doc, "site")
                if site is None:
                    site = addChild(self.doc, xml, "site")

                stanza_type_value = dn[0].split("=")[1]
                if stanza_type_value == "TIMESTAMP":
                    # dn: GlueLocationLocalID=TIMESTAMP
                    timestamp = self.getLdifValue(entry.glue['LocationPath'])
                    self.updateItem(self.doc, site, "Timestamp", timestamp)
                elif stanza_type_value == "VDT_VERSION":
                    # dn: GlueLocationLocalID=VDT_VERSION
                    vdt_version = self.getLdifValue(entry.glue['LocationVersion'])
                    self.updateItem(self.doc, site, "vdt_version", vdt_version)
                elif stanza_type_value == "GIP_VERSION":
                    # dn: GlueLocationLocalID=GIP_VERSION
                    gip_version = self.getLdifValue(entry.glue['LocationVersion'])
                    self.updateItem(self.doc, site, "gip_version", gip_version)
                elif stanza_type_value.startswith("OSG "):
                    # dn: GlueLocationLocalID=OSG 1.2.4
                    osg_version = self.getLdifValue(entry.glue['LocationVersion'])
                    osg_path = self.getLdifValue(entry.glue['LocationPath'])
                    self.updateItem(self.doc, site, "osg_version", osg_version)
                    self.updateItem(self.doc, site, "osg_path", osg_path)
            else:
                continue
        return

def main():
    xmlBuilder = SiteInfoToXml()
    xmlBuilder.main()

if __name__ == '__main__':
    main()
