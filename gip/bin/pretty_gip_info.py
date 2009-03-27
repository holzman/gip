#!/usr/bin/env python

import os
import sys
from xml.dom.minidom import Document

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_ldap import read_ldap
from gip_common import fileOverWrite, config, cp_get, getTempFilename
from gip_testing import runCommand

class SiteInfoToXml:
    def __init__(self):
        self.entries = None
        self.doc = Document()
        self.site = {"Name" : "", 
                     "Location" : "", 
                     "Coords" : "", 
                     "Policy" : "", 
                     "Sponsor" : "", 
                     "TimeStamp" : "", 
                     "Email" : "", 
                     "UserSupport" : "", 
                     "Admin" : "", 
                     "Security" : "", 
                     "cluster":[], 
                     "se":[], 
                     "service":[] 
                    }
        cp = config()
        self.xml_file = getTempFilename()
        self.xsl_file = os.path.expandvars(cp_get(cp, "gip", "pretty_gip_xsl", "$GIP_LOCATION/templates/pretty_gip_info.xsl"))
        self.html_file = os.path.expandvars(cp_get(cp, "gip", "pretty_gip_html", "$VDT_LOCATION/apache/htdocs/pretty_gip_info.html"))
        
    def main(self):
        # get and save the LD_LIBRARY_PATH
        ld_library_path = os.environ["LD_LIBRARY_PATH"]
        # clear out the LD_LIBRARY_PATH - OSG includes apache which has a version 
        # of libxml2 that is incompatible with libxslt on some systems
        os.environ["LD_LIBRARY_PATH"] = ""
        self.getEntries()
        self.parseLdif()
        fileOverWrite(self.xml_file, self.buildXml())
        self.transform()
        self.cleanup()
        # restore LD_LIBRARY_PATH
        os.environ["LD_LIBRARY_PATH"] = ld_library_path

    def cleanup(self):
        rm_cmd = "rm -rf %s" % self.xml_file
        runCommand(rm_cmd)
        
    def transform(self):
        transform_cmd = "xsltproc -o %s %s %s" % (self.html_file, self.xsl_file, self.xml_file)
        runCommand(transform_cmd)
        
    def getEntries(self):
        path = os.path.expandvars("$GIP_LOCATION/bin/gip_info")
        fd = os.popen(path)
        self.entries = read_ldap(fd, multi=True)

    def findEntry(self, entry_list, id_name, id_value):
        found = False
        found_item = None
        for item in entry_list:
            if item[id_name] == id_value:
                found = True
                found_item = item
                break
        return found, found_item
    
    def parseLdif(self):
        for entry in self.entries:
            dn = list(entry.dn)
            stanza_type = dn[0].split("=")[0]

            if stanza_type == "GlueCEUniqueID":
                # check for cluster
                cluster_id = " ".join(map(str, entry.glue['CEHostingCluster']))
                found, cluster = self.findEntry(self.site["cluster"], "Name", cluster_id)
                # if not found create with defaults
                if not found or (cluster == None):
                    cluster = {"Name" : cluster_id, "TmpDir" : "", "WNTmpDir" : "", "InformationServiceURL" : "", "subcluster":[], "ce":[] }
                    self.site["cluster"].append(cluster)
                
                policy = {"MaxWaitingJobs" : " ".join(map(str, entry.glue['CEPolicyMaxWaitingJobs'])),
                          "MaxRunningJobs" : " ".join(map(str, entry.glue['CEPolicyMaxRunningJobs'])),
                          "MaxTotalJobs" : " ".join(map(str, entry.glue['CEPolicyMaxTotalJobs'])),
                          "DefaultMaxCPUTime": " ".join(map(str, entry.glue['CEPolicyMaxCPUTime'])),
                          "MaxCPUTime" : " ".join(map(str, entry.glue['CEPolicyMaxObtainableCPUTime'])), 
                          "DefaultMaxWallClockTime" : " ".join(map(str, entry.glue['CEPolicyMaxWallClockTime'])),
                          "MaxWallClockTime" : " ".join(map(str, entry.glue['CEPolicyMaxObtainableWallClockTime'])),
                          "AssignedJobSlots" : " ".join(map(str, entry.glue['CEPolicyAssignedJobSlots'])),
                          "MaxSlotsPerJob" : " ".join(map(str, entry.glue['CEPolicyMaxSlotsPerJob'])), 
                          "Preemption" : " ".join(map(str, entry.glue['CEPolicyPreemption'])), 
                          "Priority" : " ".join(map(str, entry.glue['CEPolicyPriority'])) 
                         }

                state = {"FreeJobSlots" : " ".join(map(str, entry.glue['CEStateFreeJobSlots'])), 
                         "RunningJobs" : " ".join(map(str, entry.glue['CEStateRunningJobs'])), 
                         "WaitingJobs" : " ".join(map(str, entry.glue['CEStateWaitingJobs'])), 
                         "TotalJobs" : " ".join(map(str, entry.glue['CEStateTotalJobs'])), 
                         "ERT" : " ".join(map(str, entry.glue['CEStateEstimatedResponseTime'])), 
                         "WRT" : " ".join(map(str, entry.glue['CEStateWorstResponseTime'])) 
                        }
                
                other = {"Implementation" : " ".join(map(str, entry.glue['CEImplementationName'])), 
                         "ImplementationVersion" : " ".join(map(str, entry.glue['CEImplementationVersion'])), 
                         "CPUScalingReference" : " ".join(map(str, entry.glue['CECapability'])), 
                         "GRAMVersion" : " ".join(map(str, entry.glue['CEInfoGRAMVersion'])) 
                        }

                ce = {"Name" : " ".join(map(str, entry.glue['CEName'])), 
                      "Status" : " ".join(map(str, entry.glue['CEStateStatus'])), 
                      "Jobmanager" : "%s (%s)" % (entry.glue['CEInfoLRMSType'][0], entry.glue['CEInfoLRMSVersion'][0]), # Jobmanager (version)
                      "Port" : " ".join(map(str, entry.glue['CEInfoGatekeeperPort'])), 
                      "DataDir" : " ".join(map(str, entry.glue['CEInfoDataDir'])), 
                      "AppDir" : " ".join(map(str, entry.glue['CEInfoApplicationDir'])), 
                      "DefaultSE" : " ".join(map(str, entry.glue['CEInfoDefaultSE'])), 
                      "SupportedVOs" : ", ".join(map(str, entry.glue['CEAccessControlBaseRule'])), 
                      "policy" : policy, 
                      "state" : state, 
                      "other" : other 
                     }
                cluster["ce"].append(ce)
                
            elif stanza_type == "GlueSEUniqueID":
                se_id = " ".join(map(str, entry.glue['SEName']))
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                # if not found create with defaults
                if not found or (se == None):
                    se = {"Name" : " ".join(map(str, entry.glue['SEName'])), 
                          "Status" : " ".join(map(str, entry.glue['SEStatus'])), 
                          "Port" : " ".join(map(str, entry.glue['SEPort'])), 
                          "ImplementationName" : " ".join(map(str, entry.glue['SEImplementationName'])), 
                          "ImplementationVersion" : " ".join(map(str, entry.glue['SEImplementationVersion'])), 
                          "Architecture" : " ".join(map(str, entry.glue['SEArchitecture'])), 
                          "UsedNearlineSize" : " ".join(map(str, entry.glue['SEUsedNearlineSize'])), 
                          "UsedOnlineSize" : " ".join(map(str, entry.glue['SEUsedOnlineSize'])), 
                          "TotalNearlineSize" : " ".join(map(str, entry.glue['SETotalNearlineSize'])), 
                          "TotalOnlineSize" : " ".join(map(str, entry.glue['SETotalOnlineSize'])), 
                          "SizeFree" : " ".join(map(str, entry.glue['SESizeFree'])), 
                          "SizeTotal" : " ".join(map(str, entry.glue['SESizeTotal'])), 
                          "ControlProtocol" : [], 
                          "Door" : [], 
                          "Pool" : [] 
                         }
                    self.site["se"].append(se)
                else:
                    se["Name"] = " ".join(map(str, entry.glue['SEName'])), 
                    se["Status"] = " ".join(map(str, entry.glue['SEStatus'])), 
                    se["Port"] = " ".join(map(str, entry.glue['SEPort'])), 
                    se["ImplementationName"] = " ".join(map(str, entry.glue['SEImplementationName'])), 
                    se["ImplementationVersion"] = " ".join(map(str, entry.glue['SEImplementationVersion'])), 
                    se["Architecture"] = " ".join(map(str, entry.glue['SEArchitecture'])), 
                    se["UsedNearlineSize"] = " ".join(map(str, entry.glue['SEUsedNearlineSize'])), 
                    se["UsedOnlineSize"] = " ".join(map(str, entry.glue['SEUsedOnlineSize'])), 
                    se["TotalNearlineSize"] = " ".join(map(str, entry.glue['SETotalNearlineSize'])), 
                    se["TotalOnlineSize"] = " ".join(map(str, entry.glue['SETotalOnlineSize'])), 
                    se["SizeFree"] = " ".join(map(str, entry.glue['SESizeFree'])), 
                    se["SizeTotal"] = " ".join(map(str, entry.glue['SESizeTotal'])), 

            elif stanza_type == "GlueSALocalID":
                se_id = entry.glue['ChunkKey'][0].split("=")[1]
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                if not found or (se == None):
                    se = {"Name":se_id, "Status":"", "Port":"", "ImplementationName":"", "ImplementationVersion":"", "Architecture":"", 
                          "UsedNearlineSize":"", "UsedOnlineSize":"", "TotalNearlineSize":"", "TotalOnlineSize":"", "SizeFree":"", 
                          "SizeTotal":"", "ControlProtocol":[], "Door":[], "Pool":[]}
                    self.site["se"].append(se)

                pool = {"Name" : " ".join(map(str, entry.glue['SAName'])), 
                        "Path" : " ".join(map(str, entry.glue['SAPath'])), 
                        "Root" : " ".join(map(str, entry.glue['SARoot'])), 
                        "Quota" : " ".join(map(str, entry.glue['SAPolicyQuota'])), 
                        "MaxData": " ".join(map(str, entry.glue['SAPolicyMaxData'])), 
                        "MaxFiles" : " ".join(map(str, entry.glue['SAPolicyMaxNumFiles'])), 
                        "MaxPinDuration" : " ".join(map(str, entry.glue['SAPolicyMaxPinDuration'])), 
                        "FileLifetime" : " ".join(map(str, entry.glue['SAPolicyFileLifeTime'])), 
                        "MinMaxFileSize" : "%s/%s" % (entry.glue['SAPolicyMinFileSize'][0],entry.glue['SAPolicyMaxFileSize'][0]), 
                        "Type" : " ".join(map(str, entry.glue['SAType'])), 
                        "RetentionPolicy" : " ".join(map(str, entry.glue['SARetentionPolicy'])), 
                        "ExpirationMode" : " ".join(map(str, entry.glue['SAExpirationMode'])), 
                        "AccessLatency" : " ".join(map(str, entry.glue['SAAccessLatency'])), 
                        "SupportedVOs" : ", ".join(map(str, entry.glue['SAAccessControlBaseRule'])), 
                        "Capability" : " ".join(map(str, entry.glue['SACapability'])), 
                        "FreeOnlineSize" : " ".join(map(str, entry.glue['SAFreeOnlineSize'])), 
                        "UsedOnlineSize" : " ".join(map(str, entry.glue['SAUsedOnlineSize'])), 
                        "TotalOnlineSize" : " ".join(map(str, entry.glue['SATotalOnlineSize'])), 
                        "FreeNearlineSize" : " ".join(map(str, entry.glue['SAFreeNearlineSize'])), 
                        "UsedNearlineSize" : " ".join(map(str, entry.glue['SAUsedNearlineSize'])), 
                        "TotalNearlineSize" : " ".join(map(str, entry.glue['SATotalNearlineSize'])), 
                        "ReservedOnlineSize" : " ".join(map(str, entry.glue['SAReservedOnlineSize'])), 
                        "ReservedNearlineSize" : " ".join(map(str, entry.glue['SAReservedNearlineSize'])), 
                        "StateUsedSpace" : " ".join(map(str, entry.glue['SAStateUsedSpace'])), 
                        "StateAvailableSpace" : " ".join(map(str, entry.glue['SAStateAvailableSpace'])) 
                       }
                se["Pool"].append(pool)
                
            elif stanza_type == "GlueSEAccessProtocolLocalID":
                se_id = entry.glue['ChunkKey'][0].split("=")[1]
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                if not found or (se == None):
                    se = {"Name":se_id, "Status":"", "Port":"", "ImplementationName":"", "ImplementationVersion":"", "Architecture":"", 
                          "UsedNearlineSize":"", "UsedOnlineSize":"", "TotalNearlineSize":"", "TotalOnlineSize":"", "SizeFree":"", 
                          "SizeTotal":"", "ControlProtocol":[], "Door":[], "Pool":[]}
                    self.site["se"].append(se)

                found, door = self.findEntry(se["Door"], "Type", " ".join(map(str, entry.glue['SEAccessProtocolType'])))
                if not found or (door == None):
                    door = {"Type" : " ".join(map(str, entry.glue['SEAccessProtocolType'])), 
                            "Version" : " ".join(map(str, entry.glue['SEAccessProtocolVersion'])), 
                            "Capability" : " ".join(map(str, entry.glue['SEAccessProtocolCapability'])), 
                            "MaxStreams" : " ".join(map(str, entry.glue['SEAccessProtocolMaxStreams'])), 
                            "Port" : " ".join(map(str, entry.glue['SEAccessProtocolPort'])),
                            "SupportedSecurity" : " ".join(map(str, entry.glue['SEAccessProtocolSupportedSecurity'])), 
                            "NodeList" : []
                           }
                    se["Door"].append(door)
                
                node = entry.glue['SEAccessProtocolEndpoint'][0].split(":")[1][2:]
                door["NodeList"].append(node)

            elif stanza_type == "GlueServiceUniqueID":
                service = {}
                service["Name"] = " ".join(map(str, entry.glue['ServiceName'])) 
                service["Type"] = " ".join(map(str, entry.glue['ServiceType']))
                service["Version"] = " ".join(map(str, entry.glue['ServiceVersion']))
                try:
                    service["SupportedVOs"] = ", ".join(map(str, entry.glue['ServiceAccessControlRule']))
                except:
                    # the GUMS service won't have ServiceAccessControlRule's
                    service["SupportedVOs"] = " "
                service["StatusInfo"] = " ".join(map(str, entry.glue['ServiceStatusInfo'])) 
                service["Status"] = " ".join(map(str, entry.glue['ServiceStatus'])) 
                service["WSDL"] = " ".join(map(str, entry.glue['ServiceWSDL']))
                service["Endpoint"] = " ".join(map(str, entry.glue['ServiceEndpoint'])) 
                service["URI"] = " ".join(map(str, entry.glue['ServiceURI'])) 
                service["AccessPointURL"] = " ".join(map(str, entry.glue['ServiceAccessPointURL'])) 
                self.site["service"].append(service)
                
            elif stanza_type == "GlueSEControlProtocolLocalID":
                se_id = entry.glue['ChunkKey'][0].split("=")[1]
                found, se = self.findEntry(self.site["se"], "Name", se_id)
                if not found or (se == None):
                    se = {"Name":se_id, "Status":"", "Port":"", "ImplementationName":"", "ImplementationVersion":"", "Architecture":"", 
                          "UsedNearlineSize":"", "UsedOnlineSize":"", "TotalNearlineSize":"", "TotalOnlineSize":"", "SizeFree":"", 
                          "SizeTotal":"", "ControlProtocol":[], "Door":[], "Pool":[]}
                    self.site["se"].append(se)

                control_protocol = {"Type" : " ".join(map(str, entry.glue['SEControlProtocolType'])), 
                                    "Version" : " ".join(map(str, entry.glue['SEControlProtocolVersion'])), 
                                    "Capability" : " ".join(map(str, entry.glue['SEControlProtocolCapability'])), 
                                    "Endpoint" : " ".join(map(str, entry.glue['SEControlProtocolEndpoint']))
                                   }
                se["ControlProtocol"].append(control_protocol)

            elif stanza_type == "GlueClusterUniqueID":
                cluster_name = " ".join(map(str, entry.glue['ClusterName']))
                found, cluster = self.findEntry(self.site["cluster"], "Name", cluster_name)
                if not found or (cluster == None):
                    cluster = {"Name" : " ".join(map(str, entry.glue['ClusterName'])), 
                               "TmpDir" : " ".join(map(str, entry.glue['ClusterTmpDir'])), 
                               "WNTmpDir" : " ".join(map(str, entry.glue['ClusterWNTmpDir'])), 
                               "InformationServiceURL" : " ".join(map(str, entry.glue['InformationServiceURL'])), 
                               "subcluster":[], 
                               "ce":[] 
                              }
                    self.site["cluster"].append(cluster)
                else:
                    cluster["Name"] = " ".join(map(str, entry.glue['ClusterName']))
                    cluster["TmpDir"] = " ".join(map(str, entry.glue['ClusterTmpDir']))
                    cluster["WNTmpDir"] = " ".join(map(str, entry.glue['ClusterWNTmpDir']))
                    cluster["InformationServiceURL"] = " ".join(map(str, entry.glue['InformationServiceURL'])) 
                
            elif stanza_type == "GlueSubClusterUniqueID":
                cluster_id = entry.glue['ChunkKey'][0].split("=")[1]
                found, cluster = self.findEntry(self.site["cluster"], "Name", cluster_id)
                if not found or (cluster == None):
                    cluster = {"Name" : cluster_id, "TmpDir" : "", "WNTmpDir" : "", "InformationServiceURL" : "", "subcluster":[], "ce":[] }
                    self.site["cluster"].append(cluster)
                    
                sub_cluster = {"Name" : " ".join(map(str, entry.glue['SubClusterName'])), 
                               "CPUs" : " ".join(map(str, entry.glue['SubClusterPhysicalCPUs'])), 
                               "Cores" : " ".join(map(str, entry.glue['SubClusterLogicalCPUs'])), 
                               "Networking" : "%s/%s" % (entry.glue['HostNetworkAdapterInboundIP'][0], entry.glue['HostNetworkAdapterOutboundIP'][0]), 
                               "OS" : "%s %s %s" % (entry.glue['HostOperatingSystemName'][0], entry.glue['HostOperatingSystemRelease'][0], 
                                                    entry.glue['HostOperatingSystemVersion'][0]),
                               "CPU" : "%s %s %s" % (entry.glue['HostProcessorVendor'][0], entry.glue['HostProcessorModel'][0], 
                                                     entry.glue['HostProcessorClockSpeed'][0]), 
                               "Benchmark" : "%s/%s" % (entry.glue['HostBenchmarkSI00'][0], entry.glue['HostBenchmarkSF00'][0]), 
                               "SMPSize" : " ".join(map(str, entry.glue['HostArchitectureSMPSize'][0])), 
                               "Memory" : "%s/%s" % (entry.glue['HostMainMemoryRAMSize'][0], entry.glue['HostMainMemoryVirtualSize'][0])
                              }
                cluster["subcluster"].append(sub_cluster)

            elif stanza_type == "GlueSiteUniqueID":
                self.site["Name"] = " ".join(map(str, entry.glue['SiteName']))
                self.site["Location"] = " ".join(map(str, entry.glue['SiteLocation'][0])) 
                self.site["Coords"] = "%s/%s" % (entry.glue['SiteLongitude'][0], entry.glue['SiteLatitude'][0]) 
                self.site["Policy"] = " ".join(map(str, entry.glue['SiteWeb']))
                self.site["Sponsor"] = ", ".join(map(str, entry.glue['SiteSponsor']))
                self.site["Email"] = " ".join(map(str, entry.glue['SiteEmailContact']))
                self.site["UserSupport"] = " ".join(map(str, entry.glue['SiteUserSupportContact']))
                self.site["Admin"] = " ".join(map(str, entry.glue['SiteSysAdminContact']))
                self.site["Security"] =  " ".join(map(str, entry.glue['SiteSecurityContact']))

            elif stanza_type == "GlueLocationLocalID":
                #dn: GlueLocationLocalID=TIMESTAMP
                stanza_type_value = dn[0].split("=")[1]
                if stanza_type_value == "TIMESTAMP":
                    self.site["TimeStamp"] = " ".join(map(str, entry.glue['LocationPath'])) 

            else:
                continue

        return

    def addChild(self, leaf, child_name, text=""):
        child = self.doc.createElement(child_name)
        leaf.appendChild(child)
        text = str(text)
        if len(text) > 0:
            txtNode = self.doc.createTextNode(text)
            child.appendChild(txtNode)
        
        return child

    def buildXml(self):
        # Add Site
        site = self.addChild(self.doc, "site") 
        self.addChild(site, "siteName", self.site["Name"])
        self.addChild(site, "siteLocation", self.site["Location"])
        self.addChild(site, "siteCoords", self.site["Coords"])
        self.addChild(site, "sitePolicy", self.site["Policy"])
        self.addChild(site, "siteSponsor", self.site["Sponsor"])
        self.addChild(site, "siteTimeStamp", self.site["TimeStamp"])
        self.addChild(site, "siteEmailContact", self.site["Email"])
        self.addChild(site, "siteUserSupportContact", self.site["UserSupport"])
        self.addChild(site, "siteSysAdminContact", self.site["Admin"])
        self.addChild(site, "siteSecurityContact", self.site["Security"])
        
        # Add Services
        for site_service in self.site["service"]:  
            service = self.addChild(site, "service") 
            self.addChild(service, "serviceName", site_service["Name"])
            self.addChild(service, "serviceType", site_service["Type"])
            self.addChild(service, "serviceVersion", site_service["Version"])
            self.addChild(service, "serviceSupportedVOs", site_service["SupportedVOs"])
            self.addChild(service, "serviceStatusInfo", site_service["StatusInfo"])
            self.addChild(service, "serviceStatus", site_service["Status"])
            self.addChild(service, "serviceWSDL", site_service["WSDL"])
            self.addChild(service, "serviceEndpoint", site_service["Endpoint"])
            self.addChild(service, "serviceURI", site_service["URI"])
            self.addChild(service, "serviceAccessPointURL", site_service["AccessPointURL"])

        # Add Storage Elements
        for site_se in self.site["se"]:  
            se = self.addChild(site, "se") 
            self.addChild(se, "seName", site_se["Name"])
            self.addChild(se, "seStatus", site_se["Status"])
            self.addChild(se, "sePort", site_se["Port"])
            self.addChild(se, "seImplementationName", site_se["ImplementationName"])
            self.addChild(se, "seImplementationVersion", site_se["ImplementationVersion"])
            self.addChild(se, "seArchitecture", site_se["Architecture"])
            self.addChild(se, "seUsedNearlineSize", site_se["UsedNearlineSize"])
            self.addChild(se, "seUsedOnlineSize", site_se["UsedOnlineSize"])
            self.addChild(se, "seTotalNearlineSize", site_se["TotalNearlineSize"])
            self.addChild(se, "seTotalOnlineSize", site_se["TotalOnlineSize"])
            self.addChild(se, "seSizeFree", site_se["SizeFree"])
            self.addChild(se, "seSizeTotal", site_se["SizeTotal"])

            for cp in site_se["ControlProtocol"]:
                control_protocol = self.addChild(se, "seControlProtocol")
                self.addChild(control_protocol, "seControlProtocolType", cp["Type"])
                self.addChild(control_protocol, "seControlProtocolVersion", cp["Version"])
                self.addChild(control_protocol, "seControlProtocolCapability", cp["Capability"])
                self.addChild(control_protocol, "seControlProtocolEndpoint", cp["Endpoint"])
            
            for dr in site_se["Door"]:
                door = self.addChild(se, "seDoor")
                self.addChild(door, "seDoorType", dr["Type"])
                self.addChild(door, "seDoorVersion", dr["Version"])
                self.addChild(door, "seDoorCapability", dr["Capability"])
                self.addChild(door, "seDoorMaxStreams", dr["MaxStreams"])
                self.addChild(door, "seDoorPort", dr["Port"])
                self.addChild(door, "seDoorSupportedSecurity", dr["SupportedSecurity"])
                self.addChild(door, "seDoorNodeList", ", ".join(map(str, dr["NodeList"])))

            for pl in site_se["Pool"]:
                pool = self.addChild(se, "sePool")
                self.addChild(pool, "sePoolName", pl["Name"])
                self.addChild(pool, "sePoolPath", pl["Path"])
                self.addChild(pool, "sePoolRoot", pl["Root"])
                self.addChild(pool, "sePoolQuota", pl["Quota"])
                self.addChild(pool, "sePoolMaxData", pl["MaxData"])
                self.addChild(pool, "sePoolMaxFiles", pl["MaxFiles"])
                self.addChild(pool, "sePoolMaxPinDuration", pl["MaxPinDuration"])
                self.addChild(pool, "sePoolFileLifetime", pl["FileLifetime"])
                self.addChild(pool, "sePoolMinMaxFileSize", pl["MinMaxFileSize"])
                self.addChild(pool, "sePoolType", pl["Type"])
                self.addChild(pool, "sePoolRetentionPolicy", pl["RetentionPolicy"])
                self.addChild(pool, "sePoolExpirationMode", pl["ExpirationMode"])
                self.addChild(pool, "sePoolAccessLatency", pl["AccessLatency"])
                self.addChild(pool, "sePoolSupportedVOs", pl["SupportedVOs"])
                self.addChild(pool, "sePoolCapability", pl["Capability"])
                self.addChild(pool, "sePoolFreeOnlineSize", pl["FreeOnlineSize"])
                self.addChild(pool, "sePoolUsedOnlineSize", pl["UsedOnlineSize"])
                self.addChild(pool, "sePoolTotalOnlineSize", pl["TotalOnlineSize"])
                self.addChild(pool, "sePoolFreeNearlineSize", pl["FreeNearlineSize"])
                self.addChild(pool, "sePoolUsedNearlineSize", pl["UsedNearlineSize"])
                self.addChild(pool, "sePoolTotalNearlineSize", pl["TotalNearlineSize"])
                self.addChild(pool, "sePoolReservedOnlineSize", pl["ReservedOnlineSize"])
                self.addChild(pool, "sePoolReservedNearlineSize", pl["ReservedNearlineSize"])
                self.addChild(pool, "sePoolStateUsedSpace", pl["StateUsedSpace"])
                self.addChild(pool, "sePoolStateAvailableSpace", pl["StateAvailableSpace"])
        
        # Add site clusters
        for site_cluster in self.site["cluster"]:
            cluster = self.addChild(site, "cluster") 
            self.addChild(cluster, "clusterName", site_cluster["Name"])
            self.addChild(cluster, "clusterTmpDir", site_cluster["TmpDir"])
            self.addChild(cluster, "clusterWNTmpDir", site_cluster["WNTmpDir"])
            self.addChild(cluster, "clusterInformationServiceURL", site_cluster["InformationServiceURL"])
            
            # Add subclusters
            for sc in site_cluster["subcluster"]:
                subcluster = self.addChild(cluster, "clusterSubcluster")
                self.addChild(subcluster, "clusterSubclusterName", sc["Name"])
                self.addChild(subcluster, "clusterSubclusterCPUs", sc["CPUs"])
                self.addChild(subcluster, "clusterSubclusterCores", sc["Cores"])
                self.addChild(subcluster, "clusterSubclusterNetworking", sc["Networking"])
                self.addChild(subcluster, "clusterSubclusterOS", sc["OS"])
                self.addChild(subcluster, "clusterSubclusterCPU", sc["CPU"])
                self.addChild(subcluster, "clusterSubclusterBenchmark", sc["Benchmark"])
                self.addChild(subcluster, "clusterSubclusterSMPSize", sc["SMPSize"])
                self.addChild(subcluster, "clusterSubclusterMemory", sc["Memory"])
            
            # add ce's
            for compute_element in site_cluster["ce"]:
                ce = self.addChild(cluster, "clusterCE")
                self.addChild(ce, "clusterCEName", compute_element["Name"])
                self.addChild(ce, "clusterCEStatus", compute_element["Status"])
                self.addChild(ce, "clusterCEJobmanager", compute_element["Jobmanager"])
                self.addChild(ce, "clusterCEPort", compute_element["Port"])
                self.addChild(ce, "clusterCEDataDir", compute_element["DataDir"])
                self.addChild(ce, "clusterCEAppDir", compute_element["AppDir"])
                self.addChild(ce, "clusterCEDefaultSE", compute_element["DefaultSE"])
                self.addChild(ce, "clusterCESupportedVOs", compute_element["SupportedVOs"])

                cePolicy = compute_element["policy"]
                policy = self.addChild(ce, "clusterCEpolicy")
                self.addChild(policy, "clusterCEPolicyMaxWaitingJobs", cePolicy["MaxWaitingJobs"])
                self.addChild(policy, "clusterCEPolicyMaxRunningJobs", cePolicy["MaxRunningJobs"])
                self.addChild(policy, "clusterCEPolicyMaxTotalJobs", cePolicy["MaxTotalJobs"])
                self.addChild(policy, "clusterCEPolicyDefaultMaxCPUTime", cePolicy["DefaultMaxCPUTime"])
                self.addChild(policy, "clusterCEPolicyMaxCPUTime", cePolicy["MaxCPUTime"])
                self.addChild(policy, "clusterCEPolicyDefaultMaxWallClockTime", cePolicy["DefaultMaxWallClockTime"])
                self.addChild(policy, "clusterCEPolicyMaxWallClockTime", cePolicy["MaxWallClockTime"])
                self.addChild(policy, "clusterCEPolicyAssignedJobSlots", cePolicy["AssignedJobSlots"])
                self.addChild(policy, "clusterCEPolicyMaxSlotsPerJob", cePolicy["MaxSlotsPerJob"])
                self.addChild(policy, "clusterCEPolicyPreemption", cePolicy["Preemption"])
                self.addChild(policy, "clusterCEPolicyPriority", cePolicy["Priority"])

                ceState = compute_element["state"]
                state = self.addChild(ce, "clusterCEState")
                self.addChild(state, "clusterCEStateFreeJobSlots", ceState["FreeJobSlots"])
                self.addChild(state, "clusterCEStateRunningJobs", ceState["RunningJobs"])
                self.addChild(state, "clusterCEStateWaitingJobs", ceState["WaitingJobs"])
                self.addChild(state, "clusterCEStateTotalJobs", ceState["TotalJobs"])
                self.addChild(state, "clusterCEStateERT", ceState["ERT"])
                self.addChild(state, "clusterCEStateWRT", ceState["WRT"])
                
                ceOther = compute_element["other"]
                other = self.addChild(ce, "clusterCEOther")
                self.addChild(other, "clusterCEOtherImplementation", ceOther["Implementation"])
                self.addChild(other, "clusterCEOtherImplementationVersion", ceOther["ImplementationVersion"])
                self.addChild(other, "clusterCEOtherCPUScalingReference", ceOther["CPUScalingReference"])
                self.addChild(other, "clusterCEOtherGRAMVersion", ceOther["GRAMVersion"])

        #return self.doc.toprettyxml(indent="    ")
        return self.doc.toxml()


def main():
    xmlBuilder = SiteInfoToXml()
    xmlBuilder.main()

if __name__ == '__main__':
    main()
