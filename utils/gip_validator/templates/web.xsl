<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:template name="processUniqueValues">
    <xsl:param name="delimitedValues"/>
    <xsl:variable name="firstOne">
        <!-- variable firstOne: the first value in the delimited list of-->
        <xsl:value-of select="substring-before($delimitedValues,'~')"/>
    </xsl:variable>
    <xsl:variable name="firstOneDelimited">
        <!-- variable firstOneDelimited: the first value in the delimitedof items with the tilde "~" delimiter -->
        <xsl:value-of select="substring-before($delimitedValues,'~')"/>~
    </xsl:variable>
    <xsl:variable name="theRest">
        <!-- variable theRest: the rest of the delimited list after theone is removed -->
        <xsl:value-of select="substring-after($delimitedValues,'~')"/>
    </xsl:variable>
    <xsl:choose>
        <!-- when the current one exists again in the remaining list AND first one isn't empty, -->
        <xsl:when test="contains($theRest,$firstOneDelimited)">
            <xsl:call-template name="processUniqueValues">
                <xsl:with-param name="delimitedValues">
                    <xsl:value-of select="$theRest"/>
                </xsl:with-param>
            </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
            <!-- otherwise this is the last occurence in the list, so return item with a delimiter tilde "~". -->
            <xsl:text>
            </xsl:text>
            <tr>
                <td>
                    <h3><xsl:value-of select="$firstOne"/></h3>
                </td>
            </tr>
            <xsl:for-each select="//TestRunList/Site[@name=normalize-space($firstOne)]">
                <xsl:text>
                </xsl:text>
                <tr>
                    <td>
                    <xsl:attribute name="class"><xsl:value-of select="@result"/></xsl:attribute>
                        <a>
                            <xsl:attribute name="href">
                                <xsl:value-of select="@path"/>#<xsl:value-of select="@name"/>
                            </xsl:attribute>
                            <span style="color:#000000;"><xsl:value-of select="@test"/></span>
                        </a>
                    </td>
                </tr>
            </xsl:for-each>
            <xsl:if test="contains($theRest,'~')">
                <!-- when there are more left in the delimited list, call the with the remaining items -->
                <xsl:call-template name="processUniqueValues">
                    <xsl:with-param name="delimitedValues">
                        <xsl:value-of select="$theRest"/>
                    </xsl:with-param>
                </xsl:call-template>
            </xsl:if>
        </xsl:otherwise>
    </xsl:choose>
</xsl:template>

<xsl:template match="/">

<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
    <title>OSG - GIP Tests</title>
    <link href="css/style.css" rel="stylesheet" type="text/css" />
</head>
<body>
    <div id="container">
        <div id="top">
            <div id="logo">
                <a href="index.html"><img src="images/logo_osg.gif" alt="Open Science Grid" width="130" height="68" border="0" /></a>
            </div>
            <div id="header">GIP Toolkit</div>
        </div>
        <div id="content">
            <div id="content_body">
                <table>
                <tr>
                    <td colspan="3"><h2>Last Run: <xsl:value-of select="/TestRunList/TestRunTime"/></h2></td>
                </tr>
                <tr>
                    <td>
                        <!-- Site red/green list-->
                        <table>
                        <tr>
                            <th colspan="2">Site Status</th>
                        </tr>
                        <xsl:variable name="delimitedValues">
                            <!-- variable will result in a delimited list of all Sites. will include duplicates and is delimited with a tilde "~".
                                xml produces this:
                                ~site1~site2~site2~
                            -->
                            <xsl:for-each select="/TestRunList/Site">
                                <xsl:sort select="@name"/>
                                <xsl:value-of select="@name"/>~
                            </xsl:for-each>
                        </xsl:variable>
                        <xsl:call-template name="processUniqueValues">
                            <xsl:with-param name="delimitedValues">
                                <xsl:value-of select="$delimitedValues"/>
                            </xsl:with-param>
                        </xsl:call-template>

                        </table>
                    </td>
                    <td>
                        <div style="width : 50px;"/>
                    </td>
                    <td valign="top">
                        <table>
                        <tr>
                            <th colspan="2">Tests and Reports</th>
                        </tr>
                        <tr>
                            <td>
                                <!-- Critical test list-->
                                <table>
                                <tr>
                                    <th><h3>Critical Tests</h3></th>
                                </tr>
                                <xsl:for-each select="/TestRunList/TestDetail">
                                    <xsl:if test="@type = 'critical'">
                                        <tr>
                                            <td>
                                                <a><xsl:attribute name="href"><xsl:value-of select="@path"/></xsl:attribute>
                                                    <xsl:value-of select="current()"/>
                                                </a>
                                            </td>
                                        </tr>
                                    </xsl:if>
                                </xsl:for-each>
                                </table>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <!-- Reports -->
                                <table>
                                <tr>
                                    <th><h3>GIP Reports</h3></th>
                                </tr>
                                <xsl:for-each select="/TestRunList/TestDetail">
                                    <xsl:if test="@type = 'reports'">
                                        <tr>
                                            <td>
                                                <a><xsl:attribute name="href"><xsl:value-of select="@path"/></xsl:attribute>
                                                    <xsl:value-of select="current()"/>
                                                </a>
                                            </td>
                                        </tr>
                                    </xsl:if>
                                </xsl:for-each>
                                </table>
                            </td>
                        </tr>
                        <xsl:if test="count(//*[contains(@type,'glite')]) > 0">
                        <tr>
                            <td>
                                <!-- glite reports -->
                                <table>
                                <tr>
                                    <th><h3>gLite Reports</h3></th>
                                </tr>
                                <xsl:for-each select="/TestRunList/TestDetail">
                                    <xsl:if test="@type = 'glite'">
                                        <tr>
                                            <td>
                                                <a><xsl:attribute name="href"><xsl:value-of select="@path"/></xsl:attribute>
                                                    <xsl:value-of select="current()"/>
                                                </a>
                                            </td>
                                        </tr>
                                    </xsl:if>
                                </xsl:for-each>
                                </table>
                            </td>
                        </tr>
                        </xsl:if>
                        </table>
                    </td>
                </tr>
                </table>
            <!-- end: content -->
            </div>
            <div id="nav">
                <ul>
                    <li><a href="index.xml">Home</a></li>
                    <li><a href="http://www.opensciencegrid.org/">Open Science Grid (OSG)</a></li>
                    <li><a href="https://twiki.grid.iu.edu/bin/view/Main/WebHome">OSG TWiki</a></li>
                    <li><a href="/rsv">Local RSV results</a></li>
                </ul>
            </div>
        </div>
        <div id="footer">
            <div id="column_01">
                <a href="http://www.sc.doe.gov/" target="_blank"><img src="images/logo_nsf.gif" alt="National Science Foundation" width="27" height="27" border="0" /></a>
                <a href="http://www.sc.doe.gov/" target="_blank"><img src="images/logo_eos.gif" alt="U.S. Department of Energy's Office of Science" width="31" height="18" border="0" style="margin: 0px 0px 4px 5px;" /></a>
            </div>
            <div id="column_02">Supported by the National Science Foundation and the U.S. Department of Energy's Office of Science.</div>
        </div>
    </div>
</body>
</html>

</xsl:template>

</xsl:stylesheet>
