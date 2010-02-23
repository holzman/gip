<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template match="/">

<html>
<head>
    <link href="css/style.css" rel="stylesheet" type="text/css" />
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
    <title>OSG - Site Topology</title>
    <script>
        function toggle(objID) 
        {
            var el = document.getElementById(objID);
            if ( el.style.display == 'none' ) 
            {
                el.style.display = 'block';
            } else 
            {
                el.style.display = 'none';
            }
        } 
    </script>
</head>
<body style="background-color: FFFFFF; padding: 10px;">
    <div style="width: 100%; border: 1px solid #a8a8a8; background-color: CCCCCC;">
        <table width='100%'>
        <tr><td width='45%'>Site Name:</td><td><xsl:value-of select="/xml/site/SiteName"/></td></tr>
        <tr><td width='45%'>Site Location:</td><td><xsl:value-of select="/xml/site/SiteLocation"/></td></tr>
        <tr><td width='45%'>Site Longitude, Latitude:</td><td><xsl:value-of select="/xml/site/SiteLongitude"/>, <xsl:value-of select="/xml/site/SiteLatitude"/></td></tr>
        <tr><td width='45%'>Policy:</td><td><xsl:value-of select="/xml/site/SiteWeb"/></td></tr>
        <tr><td width='45%'>Sponsor:</td><td><xsl:value-of select="/xml/site/SiteSponsor"/></td></tr>
        <tr><td width='45%'>Last Report Time:</td><td><xsl:value-of select="/xml/site/Timestamp"/></td></tr>
        <tr><td width='45%'>VDT Version:</td><td><xsl:value-of select="/xml/site/vdt_version"/></td></tr>
        <tr><td width='45%'>GIP Version:</td><td><xsl:value-of select="/xml/site/gip_version"/></td></tr>
        <tr><td width='45%'>OSG Version:</td><td><xsl:value-of select="/xml/site/osg_version"/></td></tr>
        <tr><td width='45%'>OSG Path:</td><td><xsl:value-of select="/xml/site/osg_path"/></td></tr>
        </table>
        <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8;">
            <table width='100%'>
            <tr><td width='45%'>General Email:</td><td><xsl:value-of select="/xml/site/SiteSysAdminContact"/></td></tr>
            <tr><td width='45%'>User Support Email:</td><td><xsl:value-of select="/xml/site/SiteSysAdminContact"/></td></tr>
            <tr><td width='45%'>Admin Email:</td><td><xsl:value-of select="/xml/site/SiteSysAdminContact"/></td></tr>
            <tr><td width='45%'>Security Email:</td><td><xsl:value-of select="/xml/site/SiteSecurityContact"/></td></tr>
            </table>
        </div>
    </div>
    <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
        <p style="margin: .5% 0% .5% .5%;"><a onclick="toggle('services');">Services:</a></p>
        <hr/>
        <div id='services' style='display:none;'>
            <xsl:for-each select="/xml/service">
                <table width='100%'>
                <tr><td width='45%'>Name:</td><td><xsl:value-of select="ServiceName"/></td></tr>
                <tr><td width='45%'>UniqueID:</td><td><xsl:value-of select="ServiceUniqueID"/></td></tr>
                <tr><td width='45%'>Type:</td><td><xsl:value-of select="ServiceType"/></td></tr>
                <tr><td width='45%'>Version:</td><td><xsl:value-of select="ServiceVersion"/></td></tr>
                <tr><td width='45%'>Supported VO's:</td><td><xsl:value-of select="ServiceAccessControlRule"/></td></tr>
                <tr><td width='45%'>Status Info:</td><td><xsl:value-of select="ServiceStatusInfo"/></td></tr>
                <tr><td width='45%'>Status:</td><td><xsl:value-of select="ServiceStatus"/></td></tr>
                <tr><td width='45%'>WSDL:</td><td><xsl:value-of select="ServiceWSDL"/></td></tr>
                <tr><td width='45%'>Endpoint:</td><td><xsl:value-of select="ServiceEndpoint"/></td></tr>
                <tr><td width='45%'>URI:</td><td><xsl:value-of select="ServiceURI"/></td></tr>
                <tr><td width='45%'>Access Point URL:</td><td><xsl:value-of select="ServiceAccessPointURL"/></td></tr>
				<tr><td colspan='2'><hr/></td></tr>
				</table>
            </xsl:for-each>
        </div>        
    </div>
    <xsl:for-each select="/xml/cluster">
        <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
            <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('cluster_<xsl:value-of select="ClusterName"/>');</xsl:attribute>Cluster Name: <xsl:value-of select="ClusterName"/></a></p>
            <hr/>
            <div style='display:none;'><xsl:attribute name="id">cluster_<xsl:value-of select="ClusterName"/></xsl:attribute>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a onclick="toggle('subclusters');">Subclusters:</a></p>
                    <hr/>
                    <div id='subclusters' style='display:none;'>
                        <xsl:for-each select="subcluster">
                            <table width='100%'>
                            <tr><td width='45%'>Sub-Cluster Name:</td><td><xsl:value-of select="SubClusterName"/></td></tr>
                            <tr><td width='45%'>Number of CPU's:</td><td><xsl:value-of select="SubClusterPhysicalCPUs"/></td></tr>
                            <tr><td width='45%'>Number of CPU Cores:</td><td><xsl:value-of select="SubClusterLogicalCPUs"/></td></tr>
                            <tr><td width='45%'>Inbound/Outbound Networking:</td><td><xsl:value-of select="HostNetworkAdapterInboundIP"/>/<xsl:value-of select="HostNetworkAdapterOutboundIP"/></td></tr>
                            <tr><td width='45%'>OS Name Release version:</td><td><xsl:value-of select="HostOperatingSystemName"/> <xsl:value-of select="HostOperatingSystemRelease"/> <xsl:value-of select="HostOperatingSystemVersion"/></td></tr>
                            <tr><td width='45%'>CPU Vendor Model Clock(MHz):</td><td><xsl:value-of select="HostProcessorVendor"/> <xsl:value-of select="HostProcessorModel"/> <xsl:value-of select="HostProcessorClockSpeed"/></td></tr>
                            <tr><td width='45%'>Benchmark SI00/SF00:</td><td><xsl:value-of select="HostBenchmarkSI00"/>/<xsl:value-of select="HostBenchmarkSF00"/></td></tr>
                            <tr><td width='45%'>SMP Size:</td><td><xsl:value-of select="HostArchitectureSMPSize"/></td></tr>
                            <tr><td width='45%'>Memory RAM(MHz)/Virtual(MHz):</td><td><xsl:value-of select="HostMainMemoryRAMSize"/>/<xsl:value-of select="HostMainMemoryVirtualSize"/></td></tr>
                            <tr><td width='45%'>WN Temp Dir:</td><td><xsl:value-of select="SubClusterWNTmpDir"/></td></tr>
                            <tr><td width='45%'>Subcluster Temp Dir:</td><td><xsl:value-of select="SubClusterTmpDir"/></td></tr>
                            <tr><td colspan='2'><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <xsl:for-each select="clusterCE">
                        <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('ce_<xsl:value-of select="CEName"/>');</xsl:attribute>CE Name: <xsl:value-of select="CEName"/></a></p>
                        <hr/>
                        <div style='display:none;'><xsl:attribute name="id">ce_<xsl:value-of select="CEName"/></xsl:attribute>
                            <table width='100%'>
                            <tr><td width='45%'>CE UniqueID:</td><td><xsl:value-of select="CEUniqueID"/></td></tr>
                            <tr><td width='45%'>CE Status:</td><td><xsl:value-of select="CEStateStatus"/></td></tr>
                            <tr><td width='45%'>Jobmanager (version):</td><td><xsl:value-of select="CEInfoJobManager"/>(<xsl:value-of select="CEInfoLRMSVersion"/>)</td></tr>
                            <tr><td width='45%'>Port:</td><td><xsl:value-of select="CEInfoGatekeeperPort"/></td></tr>
                            <tr><td width='45%'>Data Directory:</td><td><xsl:value-of select="CEInfoDataDir"/></td></tr>
                            <tr><td width='45%'>Application Directory:</td><td><xsl:value-of select="CEInfoApplicationDir"/></td></tr>
                            <tr><td width='45%'>Default SE:</td><td><xsl:value-of select="CEInfoDefaultSE"/></td></tr>
                            <tr><td width='45%'>Supported VO's:</td><td><xsl:value-of select="CEAccessControlBaseRule"/></td></tr>
                            <tr><td width='45%'>Maximum Waiting Jobs:</td><td><xsl:value-of select="CEPolicyMaxWaitingJobs"/></td></tr>
                            <tr><td width='45%'>Maximum Running Jobs:</td><td><xsl:value-of select="CEPolicyMaxRunningJobs"/></td></tr>
                            <tr><td width='45%'>Maximum CPU Time:</td><td><xsl:value-of select="CEPolicyMaxCPUTime"/></td></tr>
                            <tr><td width='45%'>Maximum Wall Clock Time:</td><td><xsl:value-of select="CEPolicyMaxWallClockTime"/></td></tr>
                            <tr><td width='45%'>Maximum Slots per Job:</td><td><xsl:value-of select="CEPolicyMaxSlotsPerJob"/></td></tr>
                            <tr><td width='45%'>Assigned Job Slots:</td><td><xsl:value-of select="CEPolicyAssignedJobSlots"/></td></tr>
                            <tr><td width='45%'>Preemption:</td><td><xsl:value-of select="CEPolicyPreemption"/></td></tr>
                            <tr><td width='45%'>Priority:</td><td><xsl:value-of select="CEPolicyPriority"/></td></tr>
                            <tr><td width='45%'>Free Job Slots:</td><td><xsl:value-of select="CEStateFreeJobSlots"/></td></tr>
                            <tr><td width='45%'>Running Jobs:</td><td><xsl:value-of select="CEStateRunningJobs"/></td></tr>
                            <tr><td width='45%'>Waiting Jobs:</td><td><xsl:value-of select="CEStateWaitingJobs"/></td></tr>
                            <tr><td width='45%'>Total Jobs:</td><td><xsl:value-of select="CEStateTotalJobs"/></td></tr>
                            <tr><td width='45%'>Estimated Response Time:</td><td><xsl:value-of select="CEStateEstimatedResponseTime"/></td></tr>
                            <tr><td width='45%'>Worst Response Time:</td><td><xsl:value-of select="CEStateWorstResponseTime"/></td></tr>
                            <tr><td width='45%'>Implementation:</td><td><xsl:value-of select="CEImplementationName"/></td></tr>
                            <tr><td width='45%'>Implementation Version:</td><td><xsl:value-of select="CEImplementationVersion"/></td></tr>
                            <tr><td width='45%'>CPU Scaling Reference (SI00):</td><td><xsl:value-of select="CECapability"/></td></tr>
                            <tr><td colspan='2'><hr/></td></tr>
                            </table>
                        </div>
                    </xsl:for-each>
                </div>
            </div>
        </div>
    </xsl:for-each>
    <xsl:for-each select="/xml/se">
        <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
            <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="SEName"/>');</xsl:attribute>SE: <xsl:value-of select="SEName"/></a></p>
            <hr/>
            <div style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="SEName"/></xsl:attribute>
                <table width='100%'>
                <tr><td width='45%'>SE Unique ID:</td><td><xsl:value-of select="SEUniqueID"/></td></tr>
                <tr><td width='45%'>Status:</td><td><xsl:value-of select="SEStatus"/></td></tr>
                <tr><td width='45%'>Port:</td><td><xsl:value-of select="SEPort"/></td></tr>
                <tr><td width='45%'>Implementation Name:</td><td><xsl:value-of select="SEImplementationName"/></td></tr>
                <tr><td width='45%'>Implementation Version:</td><td><xsl:value-of select="SEImplementationVersion"/></td></tr>
                <tr><td width='45%'>Architecture:</td><td><xsl:value-of select="SEArchitecture"/></td></tr>
                </table>
                <table width='100%'>
                <tr>
                    <td width='25%'>Used Nearline Size (GB):</td><td><xsl:value-of select="SEUsedNearlineSize"/></td>
                    <td width='20%'>Used Online Size (GB):</td><td><xsl:value-of select="SEUsedOnlineSize"/></td>
                    <td width='20%'>Size Free (GB):</td><td><xsl:value-of select="SESizeFree"/></td>
                </tr>
                <tr>
                    <td width='25%'>Total Nearline Size (GB):</td><td><xsl:value-of select="SETotalNearlineSize"/></td>
                    <td width='20%'>Total Online Size (GB):</td><td><xsl:value-of select="SETotalOnlineSize"/></td>
                    <td width='20%'>Size Total (GB):</td><td><xsl:value-of select="SESizeTotal"/></td>
                </tr>
                </table>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="SEName"/>_Control_Protocols');</xsl:attribute>Control Protocols:</a></p>
                    <hr/>
                    <div style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="SEName"/>_Control_Protocols</xsl:attribute>
                        <xsl:for-each select="seControlProtocol">
                            <table width='100%'>
                            <tr><td width='45%'>Type:</td><td><xsl:value-of select="SEControlProtocolType"/></td></tr>
                            <tr><td width='45%'>Version:</td><td><xsl:value-of select="SEControlProtocolVersion"/></td></tr>
                            <tr><td width='45%'>Capability:</td><td><xsl:value-of select="SEControlProtocolCapability"/></td></tr>
                            <tr><td width='45%'>Endpoint:</td><td><xsl:value-of select="SEControlProtocolEndpoint"/></td></tr>
                            <tr><td colspan='2'><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="SEName"/>_Doors');</xsl:attribute>Doors:</a></p>
                    <hr/>
                    <div style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="SEName"/>_Doors</xsl:attribute>
                        <xsl:for-each select="door">
                            <table width='100%'>
                            <tr><td width='45%'>Type:</td><td><xsl:value-of select="SEAccessProtocolType"/></td></tr>
                            <tr><td width='45%'>Endpoint:</td><td><xsl:value-of select="SEAccessProtocolEndpoint"/></td></tr>
                            <tr><td width='45%'>Version:</td><td><xsl:value-of select="SEAccessProtocolVersion"/></td></tr>
                            <tr><td width='45%'>Capability:</td><td><xsl:value-of select="SEAccessProtocolCapability"/></td></tr>
                            <tr><td width='45%'>Max Streams:</td><td><xsl:value-of select="SEAccessProtocolMaxStreams"/></td></tr>
                            <xsl:if test='seDoorType != "dcap"'>
                                <tr><td width='45%'>Port:</td><td><xsl:value-of select="SEAccessProtocolPort"/></td></tr>
                            </xsl:if>
                            <tr><td width='45%'>Supported Security:</td><td><xsl:value-of select="SEAccessProtocolSupportedSecurity"/></td></tr>
                            </table>
                            <table width='100%'>
                            <tr><td>Node List:</td></tr>
                            <tr><td><xsl:value-of select="nodeList"/></td></tr>
                            <tr><td><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="SEName"/>_Pools');</xsl:attribute>Pools:</a></p>
                    <hr/>
                    <div id='se_Pools' style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="SEName"/>_Pools</xsl:attribute>
                        <xsl:for-each select="pool">
                            <table width='100%'>
                            <tr><td width='45%'>Pool:</td><td><xsl:value-of select="SAName"/></td></tr>
                            <tr><td width='45%'>Path:</td><td><xsl:value-of select="SAPath"/></td></tr>
                            <tr><td width='45%'>Root:</td><td><xsl:value-of select="SARoot"/></td></tr>
                            <tr><td width='45%'>Quota:</td><td><xsl:value-of select="SAPolicyQuota"/></td></tr>
                            <tr><td width='45%'>File Lifetime:</td><td><xsl:value-of select="SAPolicyFileLifeTime"/></td></tr>
                            <tr><td width='45%'>Type:</td><td><xsl:value-of select="SAType"/></td></tr>
                            <tr><td width='45%'>Retention Policy:</td><td><xsl:value-of select="SARetentionPolicy"/></td></tr>
                            <tr><td width='45%'>Expiration Mode:</td><td><xsl:value-of select="SAExpirationMode"/></td></tr>
                            <tr><td width='45%'>Access Latency:</td><td><xsl:value-of select="SAAccessLatency"/></td></tr>
                            <tr><td width='45%'>Supported VO's:</td><td><xsl:value-of select="SAAccessControlBaseRule"/></td></tr>
                            <tr><td width='45%'>Capability:</td><td>file <xsl:value-of select="SACapability"/></td></tr>
                            </table>
                            <p/>
                            <table width='100%'>
                            <tr>
                                <td width='15%'>Used Online Size (GB):</td><td><xsl:value-of select="SAUsedOnlineSize"/></td>
                                <td width='16'>Used Nearline Size (GB):</td><td><xsl:value-of select="SAUsedNearlineSize"/></td>
                                <td width='20%'>Reserved Online Size (GB):</td><td><xsl:value-of select="SAReservedOnlineSize"/></td>
                                <td width='20%'>State Used Space (KB):</td><td><xsl:value-of select="SAStateUsedSpace"/></td>
                            </tr>
                            <tr>
                                <td width='15%'>Free Online Size (GB):</td><td><xsl:value-of select="SAFreeOnlineSize"/></td>
                                <td width='16%'>Free Nearline Size (GB):</td><td><xsl:value-of select="SAFreeNearlineSize"/></td>
                                <td width='20%'>Reserved Nearline Size (GB):</td><td><xsl:value-of select="SAReservedNearlineSize"/></td>
                                <td width='20%'>State Available Space (KB):</td><td><xsl:value-of select="SAStateAvailableSpace"/></td>
                            </tr>
                            <tr>
                                <td width='15%'>Total Online Size (GB):</td><td><xsl:value-of select="SATotalOnlineSize"/></td>
                                <td width='16%'>Total Nearline Size (GB):</td><td><xsl:value-of select="SATotalNearlineSize"/></td>
                                <td colspan='4'></td>
                            </tr>
                            <tr><td colspan='8'><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
            </div>
        </div>
    </xsl:for-each>
    <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
	    <p style="margin: .5% 0% .5% .5%;"><a onclick="toggle('vos');">VO View:</a></p>
	    <hr/>
	    <div style='display:none;' id='vos'>
	        <xsl:for-each select="/xml/vo">
	            <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
	                <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('vo_<xsl:value-of select="@id"/>');</xsl:attribute>VO Name: <xsl:value-of select="@id"/></a></p>
	                <hr/>
	                <div style='display:none;'><xsl:attribute name="id">vo_<xsl:value-of select="@id"/></xsl:attribute>
                        <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('vo_<xsl:value-of select="@id"/>_CEs');</xsl:attribute>Compute Elements:</a></p>
                        <hr/>
                        <div style='display:none;'><xsl:attribute name="id">vo_<xsl:value-of select="@id"/>_CEs</xsl:attribute>
	                        <xsl:for-each select="vo_view">
			                    <table width='100%'>
			                    <tr><td>CE:</td><td><xsl:value-of select="ChunkKey"/></td></tr>
			                    <tr><td>ERT:</td><td><xsl:value-of select="CEStateEstimatedResponseTime"/></td></tr>
			                    <tr><td>WRT:</td><td><xsl:value-of select="CEStateWorstResponseTime"/></td></tr>
		                        <tr><td colspan='2'><hr style='width:75%'/></td></tr>
			                    <tr><td>CE Running Jobs:</td><td><xsl:value-of select="CEStateRunningJobs"/></td></tr>
			                    <tr><td>CE Waiting Jobs:</td><td><xsl:value-of select="CEStateWaitingJobs"/></td></tr>
			                    <tr><td>CE total Jobs:</td><td><xsl:value-of select="CEStateTotalJobs"/></td></tr>
			                    <tr><td>CE Free Job Slots:</td><td><xsl:value-of select="CEStateFreeJobSlots"/></td></tr>
		                        <tr><td colspan='2'><hr style='width:75%'/></td></tr>
			                    <tr><td>Default SE:</td><td><xsl:value-of select="CEInfoDefaultSE"/></td></tr>
			                    <tr><td>Data Directory:</td><td><xsl:value-of select="CEInfoDataDir"/></td></tr>
			                    <tr><td>Application Directory:</td><td><xsl:value-of select="CEInfoApplicationDir"/></td></tr>
			                    <tr><td colspan='2'><hr style='width:75%'/></td></tr>
		                        </table>
		                        <hr/>
	                        </xsl:for-each>
	                    </div>
                        <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('vo_<xsl:value-of select="@id"/>_Services');</xsl:attribute>Services:</a></p>
                        <hr/>
                        <div style='display:none;'><xsl:attribute name="id">vo_<xsl:value-of select="@id"/>_Services</xsl:attribute>
	                        <xsl:for-each select="service">
	                            <table width='100%'>
			                        <tr><td>Name: </td><td><xsl:value-of select="Name"/></td></tr>
			                        <tr><td>Version: </td><td><xsl:value-of select="Version"/></td></tr>
			                        <tr><td>Type: </td><td><xsl:value-of select="Type"/></td></tr>
		                            <tr><td colspan='2'><hr style='width:75%'/></td></tr>
	                            </table>
                                <hr/>
		                    </xsl:for-each>
		                </div>
		                <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('vo_<xsl:value-of select="VOViewLocalID"/>_software');</xsl:attribute><xsl:value-of select="VOViewLocalID"/> Software:</a></p>
		                <hr/>
		                <div style='display:none;'><xsl:attribute name="id">vo_<xsl:value-of select="VOViewLocalID"/>_software</xsl:attribute>
		                    <table width='100%'>
			                    <xsl:for-each select="software">
			                        <tr><td>Name: </td><td><xsl:value-of select="Name"/></td></tr>
			                        <tr><td>Version: </td><td><xsl:value-of select="Version"/></td></tr>
			                        <tr><td>Path: </td><td><xsl:value-of select="Path"/></td></tr>
	                                <tr><td colspan='2'><hr style='width:75%'/></td></tr>
			                    </xsl:for-each>
		                    </table>
		                </div>
					</div>
	            </div>        
	        </xsl:for-each>
        </div>
    </div>
</body>
</html>

</xsl:template>

</xsl:stylesheet>
