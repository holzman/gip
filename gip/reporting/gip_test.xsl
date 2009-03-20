<?xml version="1.0" encoding="ISO-8859-1"?>

<xsl:stylesheet version="1.0"
xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

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
                    <!-- content -->
                    <h3>Test Description</h3>
                    <pre><xsl:value-of select="/TestRun/TestDescription"/></pre>

                    <table rules='all' frame='border'>
                    <tr class="tableheader">
                        <th align="left">Test</th>
                        <th align="left">Result</th>
                    </tr>
                    <tr class="tableheader">
                        <th align="center" colspan="2">Message</th>
                    </tr>
                    <xsl:for-each select="/TestRun/TestCase">
                        <xsl:sort select="@name"/>
                        <xsl:variable name="current_name">
                            <xsl:value-of select="@name" />
                        </xsl:variable>
                        <tr>
                        <xsl:attribute  name="class">highlight</xsl:attribute>
                        <xsl:if test="(not(/TestRun/info)) and ((position() mod 2 = 1) and (@result!='fail'))">
                            <xsl:attribute  name="class">normal</xsl:attribute>
                        </xsl:if>
                        <xsl:if test="@result='fail'">
                            <xsl:attribute  name="class">critical</xsl:attribute>
                        </xsl:if>
                            <td><a><xsl:attribute  name="name"><xsl:value-of select="substring-after(@name,'_')"/></xsl:attribute><xsl:value-of select="@name"/></a></td>
                            <td><xsl:value-of select="@result"/></td>
                        </tr>
                        <xsl:choose>
                            <xsl:when test="@result='fail'">
                                <tr class="critical">
                                    <td colspan='2'>
                                        <xsl:for-each select="/TestRun/Failure">
                                            <xsl:sort select="@testcase"/>
                                            <xsl:if test="@testcase=$current_name">
                                                <xsl:value-of select="current()"/>
                                            </xsl:if>
                                        </xsl:for-each>
                                    </td>
                                </tr>
                            </xsl:when>
                        </xsl:choose>
                        <xsl:if test="/TestRun/info">
                            <xsl:for-each select="/TestRun/info">
                                <xsl:sort select="@testcase"/>
                                <xsl:if test="@testcase=$current_name">
                                    <tr>
                                        <td colspan='2'>
                                            <div style="overflow-y: auto !important;overflow-y: none !important;width:700px;">
                                                <pre><xsl:value-of select="current()"/></pre>
                                            </div>
                                        </td>
                                    </tr>
                                </xsl:if>
                            </xsl:for-each>
                        </xsl:if>

                    </xsl:for-each>
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
