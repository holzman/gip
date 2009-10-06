#!/usr/bin/env python

from lib.validator_base import Base
from lib.gip_ldap import read_bdii
from lib.validator_config import cp_getBoolean, cp_get
from lib.validator_common import getURLData, toBoolean
from lib.validator_common import message, MSG_CRITICAL
from lib.validator_common import passed, getTimestamp
from lib.xml_common import XMLDom

class InteropCheck(Base):
    def __init__(self, cp, query_source="gip_info"):
        Base.__init__(self, cp, query_source)
        self.myosg_url="http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&summary_attrs_showwlcg=on&all_resources=on&gridtype=on&gridtype_1=on&has_wlcg=on"
        self.interop_xml = XMLDom()
        self.interop_xml.loadXML(getURLData(self.myosg_url))
        self.itb_grid = cp_getBoolean(self.cp, "validator", "itb", False)
        if self.itb_grid: 
            self.osg_endpoint = cp_get(self.cp, "bdii", "itb_endpoint")
            self.egee_endpoint = cp_get(self.cp, "bdii", "pps_endpoint")
        else:
            self.osg_endpoint = cp_get(self.cp, "bdii", "osg_endpoint")
            self.egee_endpoint = cp_get(self.cp, "bdii", "egee_endpoint")
        self.messages = []

    def appendMessage(self, msg_type, msg_str):
        self.messages.append(message(msg_type, msg_str))

    def main(self, site):
        """
Interop Reporting Check

Checks to see if the site is registered for interoperability with the WLCG.
If the site is, then checks both the OSG BDII and the WLCG BDII to make
sure that the site has entries in both.
        """
        isInterop = self.checkIsInterop(site)
        if isInterop:
            query="(objectClass=GlueSite)"
            base="mds-vo-name=%s,mds-vo-name=local,o=grid" % site
            # try the OSG BDII
            self.cp.set('bdii', 'endpoint', self.osg_endpoint)
            data = read_bdii(self.cp, query, base)
            print data
            if len(data) < 1:
                msg = "%s does not exist in the OSG BDII" % site
                self.appendMessage(MSG_CRITICAL, msg)

            # try the WLCG BDII
            self.cp.set('bdii', 'endpoint', self.egee_endpoint)
            data = read_bdii(self.cp, query, base)
            if len(data) < 1:
                msg = "%s does not exist in the WLCG BDII" % site
                self.appendMessage(MSG_CRITICAL, msg)

        test_result = {"site"       : site, 
                       "type"       : 'OSG', 
                       "name"       : 'InteropCheck_%s' % site, 
                       "messages"   : self.messages, 
                       "timestamp"  : getTimestamp()
                      }
        if passed(self.messages): test_result["result"] = "PASS" 
        else: test_result["result"] = MSG_CRITICAL 
        self.messages = []
        return test_result

    def checkIsInterop(self, site):
        dom = self.interop_xml.getDom()
        resource_groups = dom.getElementsByTagName("ResourceGroup")
        for resource_group in resource_groups:
            GroupName = self.interop_xml.getText(resource_group.getElementsByTagName("GroupName"))
            if GroupName == site:
                resources = resource_group.getElementsByTagName("Resource")
                for resource in resources:
                    InteropBDII = self.interop_xml.getText(resource.getElementsByTagName("InteropBDII"))
                    if toBoolean(InteropBDII):
                        return True
        return False
