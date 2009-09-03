#!/usr/bin/env python

import os
import sys
import logging, logging.config, logging.handlers
from xml.dom.minidom import Document

sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/lib/python"))
sys.path.insert(0, os.path.expandvars("$GIP_LOCATION/reporting/plugins"))
from gip_common import fileOverWrite, config, cp_get, getTempFilename, ls
from gip_testing import runCommand, getTestConfig
from xml_common import getDom, getText, addChild, writeXML
from plugins_common import search_list_by_name, ConfigurationError, \
        EmptyNodeListError, GenericXMLError, getTestResultFileList

class OIM_XML:
    def __init__(self, args):
        self.cp = getTestConfig(args)
        self.oim_tests = "Missing_Sites,Interop_Reporting_Check,Validate_GIP_BDII"
        self.oim_xml_dir = "" 
        self.oim_summary_xml_file = ""
        self.oim_detail_file_template = ""
        self.results_dir = ""
        self.log = logging.getLogger("GIP.Reporting.OIM.Plugin")
        self.setConfigValues()

    def __str__ (self):
        result = []
        result.append('<OIM Plugin for GIP Reporting>')
        result.append('    %-30s %s' % ("Test Results Directory", self.results_dir))
        result.append('    %-30s %s' % ("Configured Test List", self.oim_tests))
        result.append('    %-30s %s' % ("Configured XML Directory", self.oim_xml_dir))
        result.append('    %-30s %s' % ("Summary XML File", self.oim_summary_xml_file))
        result.append('    %-30s %s' % ("Detail XML File Template", self.oim_detail_file_template))
             
        return '\n'.join(result)

    def setConfigValues(self):
        self.results_dir = cp_get(self.cp, "gip_tests", "results_dir", "UNKNOWN")
        if self.results_dir == "UNKNOWN": raise ConfigurationError("Results directory is not configured")
        self.oim_xml_dir = cp_get(self.cp, "gip_tests", "myosg_xml_dir", "UNKNOWN")
        if self.oim_xml_dir == "UNKNOWN": raise ConfigurationError("OIM XML directory is not configured")
        self.oim_summary_xml_file = "%s/%s" % (self.oim_xml_dir, cp_get(self.cp, "gip_tests", "myosg_summary_file", "myosg.xml"))
        self.oim_detail_file_template = self.oim_xml_dir + "/" + cp_get(self.cp, "gip_tests", "myosg_detail_file_template", "myosg_%s_detail.xml")
        self.oim_tests = cp_get(self.cp, "gip_tests", "myosg_tests", self.oim_tests)

    def parseIndex(self):
        index_list = []
    
        index_xml = "%s/index.xml" % self.results_dir 
        dom = getDom(index_xml, sourcetype="file")
        
        test_run_time = dom.getElementsByTagName("TestRunTime")
        test_run_time = getText(test_run_time)
        
        site_elms = dom.getElementsByTagName("Site")
        if site_elms.length > 0:
            for site in site_elms:
                site_name = site.getAttribute("name")
                test_name = site.getAttribute("test")
                result = site.getAttribute("result")
                if result.lower() == "green": result = "OK"
                if result.lower() == "red": result = "FAIL"
                
                item = search_list_by_name(site_name, index_list)
                if item == None:
                    new_item = {"Name" : site_name, "TestRunTime" : test_run_time, "TestCases" : []}
                    new_item["TestCases"].append({"Name" : test_name, "Status" : result, "Reason" : ""})
                    index_list.append(new_item)
                else:
                    item["TestCases"].append({"Name" : test_name, "Status" : result, "Reason" : ""})
           
        self.test_run_time = test_run_time 

        return index_list    
    
    def buildSummaryXML(self, index_list):
        summary_dom = Document()
        gip = addChild(summary_dom, summary_dom, "gip")
        
        addChild(summary_dom, gip, "TestRunTime", index_list[0]["TestRunTime"])

        for item in index_list:
            status = 0

            resource = addChild(summary_dom, gip, "Resource")
            addChild(summary_dom, resource, "Name", item["Name"])

            #if missing, then only show that as test result and set overallstatus to unknown
            for case in item["TestCases"]:
                if case["Name"] == "Missing_Sites" and case["Status"] != "OK":
                    testCase = addChild(summary_dom, resource, "TestCase")
                    addChild(summary_dom, testCase, "Name", case["Name"])
                    addChild(summary_dom, testCase, "Status", case["Status"])
                    addChild(summary_dom, testCase, "Reason")
                    OverAllStatus = "NA"
                    status += 1

            if status == 0:
                for case in item["TestCases"]:
                    if case["Name"] in self.oim_tests:
                        testCase = addChild(summary_dom, resource, "TestCase")
                        addChild(summary_dom, testCase, "Name", case["Name"])
                        addChild(summary_dom, testCase, "Status", case["Status"])
                        addChild(summary_dom, testCase, "Reason")
                        if not case["Status"] == "OK":
                            status += 1
            
            if status > 0:
                OverAllStatus = "FAIL"
            else:
                OverAllStatus = "OK"

            addChild(summary_dom, resource, "OverAllStatus", OverAllStatus)
            
        writeXML(summary_dom, self.oim_summary_xml_file)

    def parseResults(self):
        results_list = []
    
        # get directory listing of all xml files in results_dir
        file_list = getTestResultFileList(self.results_dir, self.oim_tests)
        # loop through the files
        for xml_file in file_list:
            dom = getDom("%s/%s" % (self.results_dir, xml_file), sourcetype="file")
            testcases = dom.getElementsByTagName("TestCase")
            for case in testcases:
                testcase_name = case.getAttribute("name") 
                resource_name = "_".join(testcase_name.split("_")[1:])
                result = case.getAttribute("result")
                
                item = search_list_by_name(resource_name, results_list)
                if item == None:
                    new_item = {"Name" : resource_name, "TestCases" : []} 
                    new_item["TestCases"].append({"Name" : xml_file[:-4], "Status" : result, "Details" : []})
                    results_list.append(new_item)
                else:
                    item["TestCases"].append({"Name" : xml_file[:-4], "Status" : result, "Details" : []})
            
            error_count = int(dom.getElementsByTagName("Error_count")[0].getAttribute("count"))
            failure_count = int(dom.getElementsByTagName("Failure_count")[0].getAttribute("count"))
            info_count = int(dom.getElementsByTagName("info_count")[0].getAttribute("count"))

            if error_count > 0:
                errors = dom.getElementsByTagName("Error")
                self.getDetailInformation(errors, results_list, xml_file[:-4], result="error")

            if failure_count > 0:
                failures = dom.getElementsByTagName("Failure")
                self.getDetailInformation(failures, results_list, xml_file[:-4], result="failure")

            if info_count > 0:
                infos = dom.getElementsByTagName("info")
                self.getDetailInformation(infos, results_list, xml_file[:-4], result="warn")
        
        return results_list
                     
    def getDetailInformation(self, elm_list, results_list, testcase_name, result="info"):
        for elm in elm_list:
            elm_name = elm.getAttribute("testcase")
            resource_name = "_".join(elm_name.split("_")[1:])
            item = search_list_by_name(resource_name, results_list)
            item_detail = search_list_by_name(testcase_name, item["TestCases"])
            if item_detail == None:
                details = [getText(elm)]
                item["TestCases"].append({"Name" : testcase_name, "Status" : result, "Details" : details})
            else:
                txt = getText([elm])
                item_detail["Status"] = result
                item_detail["Details"].append(txt)
    
    def buildResourceXML(self, results_list):
        for resource in results_list:
            resource_dom = Document()
            gip = addChild(resource_dom, resource_dom, "gip")
        
            addChild(resource_dom, gip, "TestRunTime", self.test_run_time)
            addChild(resource_dom, gip, "ResourceName", resource["Name"])
            for case in resource["TestCases"]:
                if case["Name"] in self.oim_tests:
                    testCase = addChild(resource_dom, gip, "TestCase")
                    addChild(resource_dom, testCase, "Name", case["Name"])
                    addChild(resource_dom, testCase, "Status", case["Status"])
                    for detail in case["Details"]:
                        addChild(resource_dom, testCase, "Detail", detail)
            
            writeXML(resource_dom, self.oim_detail_file_template % resource["Name"])
        
    
def main(args):
    oim_plugin = OIM_XML(args[1:])
    print >> sys.stderr, "OIM Plugin: %s" % str(oim_plugin)
    oim_plugin.buildSummaryXML(oim_plugin.parseIndex())
    oim_plugin.buildResourceXML(oim_plugin.parseResults())

if __name__ == '__main__':
    main(sys.argv)
        