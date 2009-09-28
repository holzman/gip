
site_map = {"siteName" : "SiteName",        
    "siteLocation" : "SiteLocation",         
    "siteLocation" : {"format" : "%s/%s", "names" : "SiteLongitude,SiteLatitude"},         
    "sitePolicy" : "SiteWeb",
    "siteSponsor" : "SiteSponsor",
    "siteTimeStamp" : "Timestamp",
    "siteEmailContact" : "SiteEmailContact",
    "siteUserSupportContact" : "SiteUserSupportContact",
    "siteSysAdminContact" : "SiteSysAdminContact",
    "siteSecurityContact" : "SiteSecurityContact" }

cluster_map = {"clusterName" : "ClusterName",    
   "clusterTmpDir" : "ClusterTmpDir",     
   "clusterWNTmpDir" :"ClusterWNTmpDir",    
   "clusterInformationServiceURL" : "InformationServiceURL" }

subcluster_map = {"clusterSubclusterName" : "SubClusterName", 
    "clusterSubclusterCPUs" : "SubClusterPhysicalCPUs",
    "clusterSubclusterCores" : "SubClusterLogicalCPUs",
    "clusterSubclusterNetworking" : 
        {"format" : "%s/%s", 
         "names" : "HostNetworkAdapterInboundIP,HostNetworkAdapterOutboundIP"},         
    "clusterSubclusterOS" :
        {"format" : "%s %s %s", 
         "names" : "HostOperatingSystemName,"\
                   "HostOperatingSystemRelease,"\
                   "HostOperatingSystemVersion"},         
    "clusterSubclusterCPU" :
        {"format" : "%s %s %s", 
         "names" : "HostProcessorVendor,"\
                   "HostProcessorModel,"\
                   "HostProcessorClockSpeed"},         
    "clusterSubclusterBenchmark" :                    
        {"format" : "%s/%s", 
         "names" : "HostBenchmarkSI00,HostBenchmarkSF00"},         
    "clusterSubclusterSMPSize" : "HostArchitectureSMPSize",
    "clusterSubclusterMemory" :
        {"format" : "%s/%s", 
         "names" : "HostMainMemoryRAMSize,HostMainMemoryVirtualSize"} }         

cluster_ce_map = {"clusterCEName" : "CEName", 
    "clusterCEStatus" : "CEStateStatus", 
    "clusterCEJobmanager" : 
        {"format" : "%s (%s)", "names" : "CEInfoLRMSType,CEInfoLRMSVersion"},
    "clusterCEPort" : "CEInfoGatekeeperPort",
    "clusterCEDataDir" : "CEInfoDataDir",
    "clusterCEAppDir" : "CEInfoApplicationDir",
    "clusterCEDefaultSE" : "CEInfoDefaultSE",
    "clusterCESupportedVOs" : "CEAccessControlBaseRule",
    "clusterCEPolicyMaxWaitingJobs" : "CEPolicyMaxWaitingJobs",
    "clusterCEPolicyMaxRunningJobs" : "CEPolicyMaxRunningJobs",
    "clusterCEPolicyMaxTotalJobs" : "CEPolicyMaxTotalJobs",
    "clusterCEPolicyDefaultMaxCPUTime" : "CEPolicyMaxCPUTime",
    "clusterCEPolicyMaxCPUTime" : "CEPolicyMaxObtainableCPUTime",
    "clusterCEPolicyDefaultMaxWallClockTime" : "CEPolicyMaxWallClockTime",
    "clusterCEPolicyMaxWallClockTime" : "CEPolicyMaxObtainableWallClockTime",
    "clusterCEPolicyAssignedJobSlots" : "CEPolicyAssignedJobSlots",
    "clusterCEPolicyMaxSlotsPerJob" : "CEPolicyMaxSlotsPerJob",
    "clusterCEPolicyPreemption" : "CEPolicyPreemption",
    "clusterCEPolicyPriority" : "CEPolicyPriority",
    "clusterCEStateFreeJobSlots" : "CEStateFreeJobSlots",
    "clusterCEStateRunningJobs" : "CEStateRunningJobs",
    "clusterCEStateWaitingJobs" : "CEStateWaitingJobs",
    "clusterCEStateTotalJobs" : "CEStateTotalJobs",
    "clusterCEStateERT" : "CEStateEstimatedResponseTime",
    "clusterCEStateWRT" : "CEStateWorstResponseTime",
    "clusterCEOtherImplementation" : "CEImplementationName",
    "clusterCEOtherImplementationVersion" : "CEImplementationVersion",
    "clusterCEOtherCPUScalingReference" : "CECapability",
    "clusterCEOtherGRAMVersion" : "CEInfoGRAMVersion" }

se_map = {"seName" : "SEName",     
    "seStatus" : "SEStatus",
    "sePort" : "SEPort",
    "seImplementationName" : "SEImplementationName",
    "seImplementationVersion" : "SEImplementationVersion",
    "seArchitecture" : "SEArchitecture",
    "seUsedNearlineSize" : "SEUsedNearlineSize",
    "seUsedOnlineSize" : "SEUsedOnlineSize",
    "seTotalNearlineSize" : "SETotalNearlineSize",
    "seTotalOnlineSize" : "SETotalOnlineSize",
    "seSizeFree" : "SESizeFree",
    "seSizeTotal" : "SESizeTotal" }
    
se_control_protocol_map = {"seControlProtocolType" : "SEControlProtocolType", 
    "seControlProtocolVersion" : "SEControlProtocolVersion",
    "seControlProtocolCapability" : "SEControlProtocolCapability",
    "seControlProtocolEndpoint" : "SEControlProtocolEndpoint" }

se_door_map = {"seDoorType" : "SEAccessProtocolType", 
    "seDoorVersion" : "SEAccessProtocolVersion",
    "seDoorCapability" : "SEAccessProtocolCapability",
    "seDoorMaxStreams" : "SEAccessProtocolMaxStreams",
    "seDoorPort" : "SEAccessProtocolPort",
    "seDoorSupportedSecurity" : "SEAccessProtocolSupportedSecurity",
    "seDoorNodeList" : 
        {"format" : "list", "names" : "NodeList" } }

se_pool_map = {"sePoolName" : "SAName",
    "sePoolPath" : "SAPath",
    "sePoolRoot" : "SARoot",
    "sePoolQuota" : "SAPolicyQuota",
    "sePoolMaxData" : "SAPolicyMaxData",
    "sePoolMaxFiles" : "SAPolicyMaxNumFiles",
    "sePoolMaxPinDuration" : "SAPolicyMaxPinDuration",
    "sePoolFileLifetime" : "SAPolicyFileLifeTime",
    "sePoolMinMaxFileSize" : 
        {"format": "%s/%s", "names" : "SAPolicyMinFileSize,SAPolicyMaxFileSize"},
    "sePoolType" : "SAType",
    "sePoolRetentionPolicy" : "SARetentionPolicy",
    "sePoolExpirationMode" : "SAExpirationMode",
    "sePoolAccessLatency" : "SAAccessLatency",
    "sePoolSupportedVOs" : "SAAccessControlBaseRule",
    "sePoolCapability" : "SACapability",
    "sePoolFreeOnlineSize" : "SAFreeOnlineSize",
    "sePoolUsedOnlineSize" : "SAUsedOnlineSize",
    "sePoolTotalOnlineSize" : "SATotalOnlineSize",
    "sePoolFreeNearlineSize" : "SAFreeNearlineSize",
    "sePoolUsedNearlineSize" : "SAUsedNearlineSize",
    "sePoolTotalNearlineSize" : "SATotalNearlineSize",
    "sePoolReservedOnlineSize" : "SAReservedOnlineSize",
    "sePoolReservedNearlineSize" : "SAReservedNearlineSize",
    "sePoolStateUsedSpace" : "SAStateUsedSpace",
    "sePoolStateAvailableSpace" : "SAStateAvailableSpace" }
        
service_map = {"serviceName" : "ServiceName",    
    "serviceType" : "ServiceType",
    "serviceVersion" : "ServiceVersion",
    "serviceSupportedVOs" : "ServiceAccessControlRule",
    "serviceStatusInfo" : "ServiceStatusInfo",
    "serviceStatus" : "ServiceStatus",
    "serviceWSDL" : "ServiceWSDL",
    "serviceEndpoint" : "ServiceEndpoint",
    "serviceURI" : "ServiceURI",
    "serviceAccessPointURL" : "ServiceAccessPointURL" }
