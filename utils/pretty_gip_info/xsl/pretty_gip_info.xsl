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
        <tr><td width='45%'>Site Name:</td><td><xsl:value-of select="/site/siteName"/></td></tr>
        <tr><td width='45%'>Site Location:</td><td><xsl:value-of select="/site/siteLocation"/></td></tr>
        <tr><td width='45%'>Site Longitude, Latitude:</td><td><xsl:value-of select="/site/siteCoords"/></td></tr>
        <tr><td width='45%'>Policy:</td><td><xsl:value-of select="/site/sitePolicy"/></td></tr>
        <tr><td width='45%'>Sponsor:</td><td><xsl:value-of select="/site/siteSponsor"/></td></tr>
        <tr><td width='45%'>Last Report Time:</td><td><xsl:value-of select="/site/siteTimeStamp"/></td></tr>
        </table>
        <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8;">
            <table width='100%'>
            <tr><td width='45%'>General Email:</td><td><xsl:value-of select="/site/siteEmailContact"/></td></tr>
            <tr><td width='45%'>User Support Email:</td><td><xsl:value-of select="/site/siteUserSupportContact"/></td></tr>
            <tr><td width='45%'>Admin Email:</td><td><xsl:value-of select="/site/siteSysAdminContact"/></td></tr>
            <tr><td width='45%'>Security Email:</td><td><xsl:value-of select="/site/siteSecurityContact"/></td></tr>
            </table>
        </div>
    </div>
    <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
        <p style="margin: .5% 0% .5% .5%;"><a onclick="toggle('services');">Services:</a></p>
        <hr/>
        <div id='services' style='display:none;'>
            <xsl:for-each select="/site/service">
                <table width='100%'>
                <tr><td width='45%'>Name:</td><td><xsl:value-of select="serviceName"/></td></tr>
                <tr><td width='45%'>Type:</td><td><xsl:value-of select="serviceType"/></td></tr>
                <tr><td width='45%'>Supported VO's:</td><td><xsl:value-of select="serviceSupportedVOs"/></td></tr>
                <tr><td width='45%'>Status Info:</td><td><xsl:value-of select="serviceStatusInfo"/></td></tr>
                <tr><td width='45%'>Status:</td><td><xsl:value-of select="serviceStatus"/></td></tr>
                <tr><td width='45%'>WSDL:</td><td><xsl:value-of select="serviceWSDL"/></td></tr>
                <tr><td width='45%'>Endpoint:</td><td><xsl:value-of select="serviceEndpoint"/></td></tr>
                <tr><td width='45%'>URI:</td><td><xsl:value-of select="serviceURI"/></td></tr>
                <tr><td width='45%'>Access Point URL:</td><td><xsl:value-of select="serviceAccessPointURL"/></td></tr>
				<tr><td colspan='2'><hr/></td></tr>
				</table>
				<p/>
            </xsl:for-each>
        </div>        
    </div>
    <xsl:for-each select="/site/cluster">
        <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
            <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('cluster_<xsl:value-of select="clusterName"/>');</xsl:attribute>Cluster Name: <xsl:value-of select="clusterName"/></a></p>
            <hr/>
            <div style='display:none;'><xsl:attribute name="id">cluster_<xsl:value-of select="clusterName"/></xsl:attribute>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a onclick="toggle('subclusters');">Subclusters:</a></p>
                    <hr/>
                    <div id='subclusters' style='display:none;'>
                        <xsl:for-each select="clusterSubcluster">
                            <table width='100%'>
                            <tr><td width='45%'>Sub-Cluster Name:</td><td><xsl:value-of select="clusterSubclusterName"/></td></tr>
                            <tr><td width='45%'>Number of CPU's:</td><td><xsl:value-of select="clusterSubclusterCPUs"/></td></tr>
                            <tr><td width='45%'>Number of CPU Cores:</td><td><xsl:value-of select="clusterSubclusterCores"/></td></tr>
                            <tr><td width='45%'>Inbound/Outbound Networking:</td><td><xsl:value-of select="clusterSubclusterNetworking"/></td></tr>
                            <tr><td width='45%'>OS Name Release version:</td><td><xsl:value-of select="clusterSubclusterOS"/></td></tr>
                            <tr><td width='45%'>CPU Vendor Model Clock(MHz):</td><td><xsl:value-of select="clusterSubclusterCPU"/></td></tr>
                            <tr><td width='45%'>Benchmark SI00/SF00:</td><td><xsl:value-of select="clusterSubclusterBenchmark"/></td></tr>
                            <tr><td width='45%'>SMP Size:</td><td><xsl:value-of select="clusterSubclusterSMPSize"/></td></tr>
                            <tr><td width='45%'>Memory RAM(MHz)/Virtual(MHz):</td><td><xsl:value-of select="clusterSubclusterMemory"/></td></tr>
                            <tr><td colspan='2'><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <xsl:for-each select="clusterCE">
                        <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('ce_<xsl:value-of select="clusterCEName"/>');</xsl:attribute>CE Name: <xsl:value-of select="clusterCEName"/></a></p>
                        <hr/>
                        <div style='display:none;'><xsl:attribute name="id">ce_<xsl:value-of select="clusterCEName"/></xsl:attribute>
                            <table width='100%'>
                            <tr><td width='45%'>CE Status:</td><td><xsl:value-of select="clusterCEStatus"/></td></tr>
                            <tr><td width='45%'>Jobmanager (version):</td><td><xsl:value-of select="clusterCEJobmanager"/></td></tr>
                            <tr><td width='45%'>Port:</td><td><xsl:value-of select="clusterCEPort"/></td></tr>
                            <tr><td width='45%'>Data Directory:</td><td><xsl:value-of select="clusterCEDataDir"/></td></tr>
                            <tr><td width='45%'>Application Directory:</td><td><xsl:value-of select="clusterCEAppDir"/></td></tr>
                            <tr><td width='45%'>Default SE:</td><td><xsl:value-of select="clusterCEDefaultSE"/></td></tr>
                            <tr><td width='45%'>Supported VO's:</td><td><xsl:value-of select="clusterCESupportedVOs"/></td></tr>
                            <tr><td width='45%'>Maximum Waiting Jobs:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyMaxWaitingJobs"/></td></tr>
                            <tr><td width='45%'>Maximum Running Jobs:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyMaxRunningJobs"/></td></tr>
                            <tr><td width='45%'>Default Maximum CPU Time:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyDefaultMaxCPUTime"/></td></tr>
                            <tr><td width='45%'>Maximum CPU Time:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyMaxCPUTime"/></td></tr>
                            <tr><td width='45%'>Default Maximum Wall Clock Time:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyDefaultMaxWallClockTime"/></td></tr>
                            <tr><td width='45%'>Maximum Wall Clock Time:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyMaxWallClockTime"/></td></tr>
                            <tr><td width='45%'>Assigned Job Slots:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyAssignedJobSlots"/></td></tr>
                            <tr><td width='45%'>Maximum Slots per Job:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyMaxSlotsPerJob"/></td></tr>
                            <tr><td width='45%'>Preemption:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyPreemption"/></td></tr>
                            <tr><td width='45%'>Priority:</td><td><xsl:value-of select="clusterCEpolicy/clusterCEPolicyPriority"/></td></tr>
                            <tr><td width='45%'>Free Job Slots:</td><td><xsl:value-of select="clusterCEState/clusterCEStateFreeJobSlots"/></td></tr>
                            <tr><td width='45%'>Running Jobs:</td><td><xsl:value-of select="clusterCEState/clusterCEStateRunningJobs"/></td></tr>
                            <tr><td width='45%'>Waiting Jobs:</td><td><xsl:value-of select="clusterCEState/clusterCEStateWaitingJobs"/></td></tr>
                            <tr><td width='45%'>Total Jobs:</td><td><xsl:value-of select="clusterCEState/clusterCEStateTotalJobs"/></td></tr>
                            <tr><td width='45%'>Estimated Response Time:</td><td><xsl:value-of select="clusterCEState/clusterCEStateERT"/></td></tr>
                            <tr><td width='45%'>Worst Response Time:</td><td><xsl:value-of select="clusterCEState/clusterCEStateWRT"/></td></tr>
                            <tr><td width='45%'>Implementation:</td><td><xsl:value-of select="clusterCEOther/clusterCEOtherImplementation"/></td></tr>
                            <tr><td width='45%'>Implementation Version:</td><td><xsl:value-of select="clusterCEOther/clusterCEOtherImplementationVersion"/></td></tr>
                            <tr><td width='45%'>CPU Scaling Reference (SI00):</td><td><xsl:value-of select="clusterCEOther/clusterCEOtherCPUScalingReference"/></td></tr>
                            <tr><td colspan='2'><hr/></td></tr>
                            </table>
                        </div>
                    </xsl:for-each>
                </div>
            </div>
        </div>
    </xsl:for-each>
    <xsl:for-each select="/site/se">
        <div style="width: 100%; border: 1px solid #a8a8a8; background-color: DDDDDD;">
            <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="seName"/>');</xsl:attribute>SE: <xsl:value-of select="seName"/></a></p>
            <hr/>
            <div style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="seName"/></xsl:attribute>
                <table width='100%'>
                <tr><td width='45%'>Status:</td><td><xsl:value-of select="seStatus"/></td></tr>
                <tr><td width='45%'>Port:</td><td><xsl:value-of select="sePort"/></td></tr>
                <tr><td width='45%'>Implementation Name:</td><td><xsl:value-of select="seImplementationName"/></td></tr>
                <tr><td width='45%'>Implementation Version:</td><td><xsl:value-of select="seImplementationVersion"/></td></tr>
                <tr><td width='45%'>Architecture:</td><td><xsl:value-of select="seArchitecture"/></td></tr>
                </table>
                <table width='100%'>
                <tr>
                    <td width='25%'>Used Nearline Size (GB):</td><td><xsl:value-of select="seUsedNearlineSize"/></td>
                    <td width='20%'>Used Online Size (GB):</td><td><xsl:value-of select="seUsedOnlineSize"/></td>
                    <td width='20%'>Size Free (GB):</td><td><xsl:value-of select="seSizeFree"/></td>
                </tr>
                <tr>
                    <td width='25%'>Total Nearline Size (GB):</td><td><xsl:value-of select="seTotalNearlineSize"/></td>
                    <td width='20%'>Total Online Size (GB):</td><td><xsl:value-of select="seTotalOnlineSize"/></td>
                    <td width='20%'>Size Total (GB):</td><td><xsl:value-of select="seSizeTotal"/></td>
                </tr>
                </table>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="seName"/>_Control_Protocols');</xsl:attribute>Control Protocols:</a></p>
                    <hr/>
                    <div style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="seName"/>_Control_Protocols</xsl:attribute>
                        <xsl:for-each select="seControlProtocol">
                            <table width='100%'>
                            <tr><td width='45%'>Type:</td><td><xsl:value-of select="seControlProtocolType"/></td></tr>
                            <tr><td width='45%'>Version:</td><td><xsl:value-of select="seControlProtocolVersion"/></td></tr>
                            <tr><td width='45%'>Capability:</td><td><xsl:value-of select="seControlProtocolCapability"/></td></tr>
                            <tr><td width='45%'>Endpoint:</td><td><xsl:value-of select="seControlProtocolEndpoint"/></td></tr>
                            <tr><td colspan='2'><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="seName"/>_Doors');</xsl:attribute>Doors:</a></p>
                    <hr/>
                    <div style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="seName"/>_Doors</xsl:attribute>
                        <xsl:for-each select="seDoor">
                            <table width='100%'>
                            <tr><td width='45%'>Type:</td><td><xsl:value-of select="seDoorType"/></td></tr>
                            <tr><td width='45%'>Version:</td><td><xsl:value-of select="seDoorVersion"/></td></tr>
                            <tr><td width='45%'>Capability:</td><td><xsl:value-of select="seDoorCapability"/></td></tr>
                            <tr><td width='45%'>Max Streams:</td><td><xsl:value-of select="seDoorMaxStreams"/></td></tr>
                            <xsl:if test='seDoorType != "dcap"'>
                                <tr><td width='45%'>Port:</td><td><xsl:value-of select="seDoorPort"/></td></tr>
                            </xsl:if>
                            <tr><td width='45%'>Supported Security:</td><td><xsl:value-of select="seDoorSupportedSecurity"/></td></tr>
                            </table>
                            <table width='100%'>
                            <tr><td>Node List:</td></tr>
                            <tr><td><xsl:value-of select="seDoorNodeList"/></td></tr>
                            <tr><td><hr/></td></tr>
                            </table>
                            <p/>
                        </xsl:for-each>
                    </div>
                </div>
                <div style="width: 99%; margin: 0% 0% .5% .5%; border: 1px solid #a8a8a8; background-color: EEEEEE;">
                    <p style="margin: .5% 0% .5% .5%;"><a><xsl:attribute name="onclick">toggle('se_<xsl:value-of select="seName"/>_Pools');</xsl:attribute>Pools:</a></p>
                    <hr/>
                    <div id='se_Pools' style='display:none;'><xsl:attribute name="id">se_<xsl:value-of select="seName"/>_Pools</xsl:attribute>
                        <xsl:for-each select="sePool">
                            <table width='100%'>
                            <tr><td width='45%'>Pool:</td><td><xsl:value-of select="sePoolName"/></td></tr>
                            <tr><td width='45%'>Path:</td><td><xsl:value-of select="sePoolPath"/></td></tr>
                            <tr><td width='45%'>Root:</td><td><xsl:value-of select="sePoolRoot"/></td></tr>
                            <tr><td width='45%'>Quota:</td><td><xsl:value-of select="sePoolQuota"/></td></tr>
                            <tr><td width='45%'>Maximum Data:</td><td><xsl:value-of select="sePoolMaxData"/></td></tr>
                            <tr><td width='45%'>Maximum Number Files:</td><td><xsl:value-of select="sePoolMaxFiles"/></td></tr>
                            <tr><td width='45%'>Maximum Pin Duration:</td><td><xsl:value-of select="sePoolMaxPinDuration"/></td></tr>
                            <tr><td width='45%'>File Lifetime:</td><td><xsl:value-of select="sePoolFileLifetime"/></td></tr>
                            <tr><td width='45%'>Minimum/Maximum File Size:</td><td><xsl:value-of select="sePoolMinMaxFileSize"/></td></tr>
                            <tr><td width='45%'>Type:</td><td><xsl:value-of select="sePoolType"/></td></tr>
                            <tr><td width='45%'>Retention Policy:</td><td><xsl:value-of select="sePoolRetentionPolicy"/></td></tr>
                            <tr><td width='45%'>Expiration Mode:</td><td><xsl:value-of select="sePoolExpirationMode"/></td></tr>
                            <tr><td width='45%'>Access Latency:</td><td><xsl:value-of select="sePoolAccessLatency"/></td></tr>
                            <tr><td width='45%'>Supported VO's:</td><td><xsl:value-of select="sePoolSupportedVOs"/></td></tr>
                            <tr><td width='45%'>Capability:</td><td>file <xsl:value-of select="sePoolCapability"/></td></tr>
                            </table>
                            <p/>
                            <table width='100%'>
                            <tr>
                                <td width='15%'>Used Online Size (GB):</td><td><xsl:value-of select="sePoolUsedOnlineSize"/></td>
                                <td width='16'>Used Nearline Size (GB):</td><td><xsl:value-of select="sePoolUsedNearlineSize"/></td>
                                <td width='20%'>Reserved Online Size (GB):</td><td><xsl:value-of select="sePoolReservedOnlineSize"/></td>
                                <td width='20%'>State Used Space (KB):</td><td><xsl:value-of select="sePoolStateUsedSpace"/></td>
                            </tr>
                            <tr>
                                <td width='15%'>Free Online Size (GB):</td><td><xsl:value-of select="sePoolFreeOnlineSize"/></td>
                                <td width='16%'>Free Nearline Size (GB):</td><td><xsl:value-of select="sePoolFreeNearlineSize"/></td>
                                <td width='20%'>Reserved Nearline Size (GB):</td><td><xsl:value-of select="sePoolReservedNearlineSize"/></td>
                                <td width='20%'>State Available Space (KB):</td><td><xsl:value-of select="sePoolStateAvailableSpace"/></td>
                            </tr>
                            <tr>
                                <td width='15%'>Total Online Size (GB):</td><td><xsl:value-of select="sePoolTotalOnlineSize"/></td>
                                <td width='16%'>Total Nearline Size (GB):</td><td><xsl:value-of select="sePoolTotalNearlineSize"/></td>
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
</body>
</html>

</xsl:template>

</xsl:stylesheet>
