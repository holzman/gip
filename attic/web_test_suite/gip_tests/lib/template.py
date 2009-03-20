import os
import sys

pageHeader = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
    <title>%s</title>
    <link href="../css/uscms.css" rel="stylesheet" type="text/css" media="screen" />
    <link href="../css/menu.css" rel="stylesheet" type="text/css">
    <link href="../css/menu_core.css" rel="stylesheet" type="text/css">
    <link href="../css/uscms_print.css" rel="stylesheet" type="text/css" media="print" />
    <script language="javascript" src="../js/menu.js" type="text/javascript"></script>
</head>
<body>
    <div id="container">
        <div id="top">
            <div id="logo">
                <a href="http://www.uscms.org/index.shtml">
                    <img src="../images_2/logo_uscms.gif" alt="U.S. CMS" width="265" height="41" border="0" />
                </a>
            </div>
        </div>
        <div id="container2_subpage">
        <!-- container2 -->
            <div id="nav_column">
                <div id="imenuscontainer">
                    <div class="imcm imde" id="imouter0">
                        <ul id="imenus0">
                            <li>
                                <a href="http://home.fnal.gov/~tiradani/">Tiradani Home</a>
                            </li>
                            <li>
                                <a href="http://www.uscms.org/index.shtml">U.S. CMS Home</a>
                            </li>
                            <div class="imea imeam"><div>
                        </ul>
                    </div>
                </div>
            </div>
            <div id="content">
"""

pageFooter = """
            </div>
            <!-- footer -->
            <div id="footer">
                <div>
                    U.S. CMS is supported by the
                    <a href="http://www.er.doe.gov/" target="_blank">
                        <img src="../images_2/logo_usdoe.gif" alt="U.S. Department of Energy" align="absmiddle" hspace="5" width="25" height="14" border="0" />
                    </a>
                    U.S. Department of Energy and the
                    <a href="http://www.nsf.gov/" target="_blank">
                        <img src="../images_2/logo_nsf.gif" alt="National Science Foundation" align="absmiddle" hspace="5" width="24" height="24" border="0" />
                    </a>
                    National Science Foundation.
            </div>
            <!-- end: footer -->
        </div>
        <!-- end: container2 -->
        </div>
    </div>
</body>
</html>
"""
def getPageHeader(title="GIP Toolkit - ITB"):
    return pageHeader % title

def getPageFooter():
    return pageFooter
