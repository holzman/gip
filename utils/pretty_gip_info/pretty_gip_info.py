#!/usr/bin/env python
# pylint: disable-msg=E1101
import os
from pretty_gip_info_common import safeGet, LdifToXML, write_file, getTempFilename
from pretty_gip_info_map import site_map, cluster_map, subcluster_map
from pretty_gip_info_map import cluster_ce_map, se_map, se_control_protocol_map
from pretty_gip_info_map import se_door_map, se_pool_map, service_map

class SiteInfoToXml(LdifToXML):
    def __init__(self):
        LdifToXML.__init__(self, source="gip", source_path="")
        self.site = {"Name" : "", "cluster":[], "se":[], "service":[]}
        self.xml_file = getTempFilename()
        self.xsl_file = "$GIP_LOCATION/templates/pretty_gip_info.xsl"
        self.html_file = "$VDT_LOCATION/apache/htdocs/pretty_gip_info.html" 
        
    def main(self):
        # get and save the LD_LIBRARY_PATH
        try:
            ld_library_path = os.environ["LD_LIBRARY_PATH"]
        except KeyError:
            ld_library_path = ""
            
        # clear out the LD_LIBRARY_PATH - OSG includes apache which has a version 
        # of libxml2 that is incompatible with libxslt on some systems
        os.environ["LD_LIBRARY_PATH"] = ""
        
        self.parseLdif()
        print self.buildXml()
        write_file(self.xml_file, self.buildXml())
        self.transform(self.html_file, self.xsl_file, self.xml_file)
        self.cleanup()
        
        # restore LD_LIBRARY_PATH
        if len(ld_library_path) > 0:
            os.environ["LD_LIBRARY_PATH"] = ld_library_path

    def runCommand(self, cmd):
        return os.popen(cmd)

    def cleanup(self):
        rm_cmd = "rm -rf %s" % self.xml_file
        self.runCommand(rm_cmd)
        
    def findEntry(self, entry_list, id_name, id_value):
        found = False
        found_item = None
        for item in entry_list:
            if item[id_name] == id_value:
                found = True
                found_item = item
                break
        return found, found_item
    
    def fillDictionary(self, glue_entries, new_dict):
        entry_keys = glue_entries.keys()
        for k in entry_keys:
            new_dict[k] = glue_entries[k]
           
    def parseLdif(self):
        for entry in self.entries:
            dn = list(entry.dn)
            stanza_type = dn[0].split("=")[0]

            if stanza_type == "GlueCEUniqueID":
                # check for cluster
                cluster_id = safeGet(entry.glue, 'CEHostingCluster')
                found, cluster = self.findEntry(self.site["cluster"], "Name", cluster_id)
                # if not found create with defaults
                if not found or (cluster == None):
                    cluster = {"Name":cluster_id, "subcluster":[], "ce":[]}
                    self.site["cluster"].append(cluster)
                
                ce = {}
                self.fillDictionary(entry.glue, ce)
                cluster["ce"].append(ce)
                
            elif stanza_type == "GlueSEUniqueID":
                se_id = safeGet(entry.glue, 'SEName')
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                # if not found create with defaults
                if not found or (se == None):
                    se = {"Name":se_id,"ControlProtocol":[],"Door":[],"Pool":[]} 
                    self.site["se"].append(se)

                self.fillDictionary(entry.glue, se)

            elif stanza_type == "GlueSALocalID":
                se_id = safeGet(entry.glue, 'ChunkKey').split("=")[1]
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                if not found or (se == None):
                    se = {"Name":se_id,"ControlProtocol":[],"Door":[],"Pool":[]} 
                    self.site["se"].append(se)
                
                pool = {}
                self.fillDictionary(entry.glue, pool)
                se["Pool"].append(pool)
                
            elif stanza_type == "GlueSEAccessProtocolLocalID":
                se_id = safeGet(entry.glue, 'ChunkKey').split("=")[1]
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                if not found or (se == None):
                    se = {"Name":se_id,"ControlProtocol":[],"Door":[],"Pool":[]}
                    self.site["se"].append(se)

                ProtocolType = safeGet(entry.glue, 'SEAccessProtocolType')
                found, door = self.findEntry(se["Door"], "Type", ProtocolType)
                if not found or (door == None):
                    door = {"Type" : ProtocolType, "NodeList" : []}
                    self.fillDictionary(entry.glue, door)
                    se["Door"].append(door)
                
                node = safeGet(entry.glue, 'SEAccessProtocolEndpoint').split(":")[1][2:]
                door["NodeList"].append(node)

            elif stanza_type == "GlueServiceUniqueID":
                service = {}
                self.fillDictionary(entry.glue, service)
                # the GUMS service won't have ServiceAccessControlRule's so see if it 
                try:
                    service["SupportedVOs"]
                except KeyError:
                    service["SupportedVOs"] = " "

                self.site["service"].append(service)
                
            elif stanza_type == "GlueSEControlProtocolLocalID":
                se_id = safeGet(entry.glue, 'ChunkKey').split("=")[1]
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                if not found or (se == None):
                    se = {"Name":se_id,"ControlProtocol":[],"Door":[],"Pool":[]}
                    self.site["se"].append(se)

                control_protocol = {}
                self.fillDictionary(entry.glue, control_protocol)
                se["ControlProtocol"].append(control_protocol)

            elif stanza_type == "GlueClusterUniqueID":
                cluster_name = safeGet(entry.glue, 'ClusterName')
                found, cluster = self.findEntry(self.site["cluster"], "Name", cluster_name)
                if not found or (cluster == None):
                    cluster = {"Name" : cluster_name, "subcluster":[], "ce":[]}
                    self.site["cluster"].append(cluster)

                self.fillDictionary(entry.glue, cluster)
                    
            elif stanza_type == "GlueSubClusterUniqueID":
                cluster_id = safeGet(entry.glue, 'ChunkKey').split("=")[1]
                found, cluster = self.findEntry(self.site["cluster"], "Name", cluster_id)
                if not found or (cluster == None):
                    cluster = {"Name" : cluster_id, "subcluster":[], "ce":[]}
                    self.site["cluster"].append(cluster)
                
                sub_cluster = {}
                self.fillDictionary(entry.glue, sub_cluster)
                cluster["subcluster"].append(sub_cluster)

            elif stanza_type == "GlueSiteUniqueID":
                self.site["Name"] = safeGet(entry.glue, 'SiteName')
                self.fillDictionary(entry.glue, self.site)
                
            elif stanza_type == "GlueLocationLocalID":
                #dn: GlueLocationLocalID=TIMESTAMP
                stanza_type_value = dn[0].split("=")[1]
                if stanza_type_value == "TIMESTAMP":
                    self.site["TimeStamp"] = safeGet(entry.glue, 'LocationPath') 

            else:
                continue

        return

    def mapStanza(self, stanza_map, source, child):
        stanza_keys = stanza_map.iterkeys()
        for key in stanza_keys:
            value = self.MapLdif(stanza_map, key, source)
            self.addChild(child, key, value) 

    def buildXml(self):
        # Add Site
        child_site = self.addChild(self.doc, "site") 
        self.mapStanza(site_map, self.site, child_site)

        # Add Services
        for site_service in self.site["service"]:  
            child_service = self.addChild(child_site, "service") 
            self.mapStanza(service_map, site_service, child_service)

        # Add Storage Elements
        for site_se in self.site["se"]:  
            child_se = self.addChild(child_site, "se") 
            self.mapStanza(se_map, site_se, child_se)

            for cp in site_se["ControlProtocol"]:
                control_protocol = self.addChild(child_se, "seControlProtocol")
                self.mapStanza(se_control_protocol_map, cp, control_protocol)
            
            for dr in site_se["Door"]:
                door = self.addChild(child_se, "seDoor")
                self.mapStanza(se_door_map, dr, door)

            for pl in site_se["Pool"]:
                pool = self.addChild(child_se, "sePool")
                self.mapStanza(se_pool_map, pl, pool)
        
        # Add site clusters
        for site_cluster in self.site["cluster"]:
            cluster = self.addChild(child_site, "cluster") 
            self.mapStanza(cluster_map, site_cluster, cluster)
            
            # Add subclusters
            for sc in site_cluster["subcluster"]:
                subcluster = self.addChild(cluster, "clusterSubcluster")
                self.mapStanza(subcluster_map, sc, subcluster)
            
            # add ce's
            for compute_element in site_cluster["ce"]:
                ce = self.addChild(cluster, "clusterCE")
                self.mapStanza(cluster_ce_map, compute_element, ce)
        return self.doc.toxml()

def main():
    xmlBuilder = SiteInfoToXml()
    xmlBuilder.main()

if __name__ == '__main__':
    main()
