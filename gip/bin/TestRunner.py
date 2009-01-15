#!/usr/bin/env python

import os
import sys
import time
from shutil import copy

sys.path.append(os.path.expandvars("$GIP_LOCATION/lib/python"))
from gip_common import config, cp_get, cp_getBoolean, fileOverWrite, ls
from gip_testing import runCommand, getTestConfig
from gip_report_sax_handler import GipResultsParser
from xml_common import parseXmlSax

class TestRunner:
    def __init__(self):
        self.cp = getTestConfig("")

        # these are the test lists by category
        self.reports = []
        self.critical_tests = []
        self.glite_tests = []

        # output files
        self.output_files = []

        # command line values
        self.output_dir = cp_get(self.cp, "gip_tests", "results_dir", os.path.expandvars("$GIP_LOCATION/reporting/results"))
        self.source_cmd = "source %s/setup.sh" % os.path.expandvars("$VDT_LOCATION")
        self.extList = [".xsl"]

        # set up the critical test list
        default_critical_tests = "Interop_Reporting_Check,Missing_Sites,Validate_GIP_BDII,Validate_GIP_URL"
        crit_conf = cp_get(self.cp, "gip_tests", "critical_tests", default_critical_tests)
        self.crit = [i.strip() for i in crit_conf.split(',')]

    def writeResultsPage(self):
        contents = '<?xml version="1.0" encoding="UTF-8"?>\n'
        contents += '<?xml-stylesheet type="text/xsl" href="index.xsl"?>\n'
        contents += '<TestRunList>\n'
        siteTestStatus = ""
        updateDateTime = time.strftime("%a %b %d %T UTC %Y", time.gmtime())
        contents += "<TestRunTime><![CDATA[%s]]></TestRunTime>\n" % updateDateTime

        for dict in self.output_files:
            file = dict["file"]
            type = dict["type"]
            fullPathItems = file.split("/")
            display = os.path.basename(file).replace("_", " ")[:-4]
            contents += "<TestDetail path='%s' type='%s'>%s</TestDetail>\n" % (os.path.basename(file), type, display)

            if type == 'critical':
                xml = open(file, "r")
                handler = GipResultsParser()
                parseXmlSax(xml, handler)
                crit_test_results = handler.getGipResults()
                cases = crit_test_results["cases"]
                for case in cases:
                    sitename = case["site"]
                    count = int(case["failure_count"]) + int(case["error_count"]) + int(case["info_count"])
                    if count > 0:
                        result = "red"
                    else:
                        result = "green"
                    siteTestStatus += "<Site name='%s' test='%s' result='%s' path='%s'/>\n" % (sitename, os.path.basename(file)[:-4], result, os.path.basename(file))

        # need to build site status table here
        contents += siteTestStatus

        contents += '</TestRunList>'
        output_file = "%s/index.xml" % self.output_dir
        fileOverWrite(output_file, contents)

        return output_file

    def write_results(self, test, contents):
        output_file = "%s/%s.xml" % (self.output_dir, os.path.basename(test))
        fileOverWrite(output_file, contents)

        return output_file

    def getTests(self):
        test_dir = os.path.expandvars("$GIP_LOCATION/reporting")
        test_list = ls(test_dir)
        for test in test_list:
            if test in self.crit:
                self.critical_tests.append("%s/%s" % (test_dir, test))
            else:
                self.reports.append("%s/%s" % (test_dir, test))

        if cp_getBoolean(self.cp, "gip_tests", "enable_glite"):
            test_dir = os.path.expandvars("$GIP_LOCATION/reporting/glite_reports")
            tmp_list = ls(test_dir)
            for i in range(0, len(tmp_list)):
                tmp_list[i] = "%s/%s" % (test_dir, tmp_list[i])
            self.glite_tests.extend(tmp_list)

    def runList(self, list, type):
        for test in list:
            # check if entry is a directory... don't try to execute directories
            if not os.path.isdir(test):
                # check if the entry is an .xsl file, don't execute, just copy to the output directory
                if os.path.splitext(test)[1] in self.extList:
                    copy(test, self.output_dir)
                    continue

                # Ok, not a directory, and not an .xsl file, we can be reasonably sure that this is an actual test... *now* execute it
                cmd = '/bin/bash -c "%(source)s; %(test)s %(format)s"' % ({"source": self.source_cmd, "test": test, "format":"xml"})
                print >> sys.stderr, "Running %s" % cmd

                output = runCommand(cmd).read()
                output_file = self.write_results(test, output)
                self.output_files.append({"file" : output_file, "type" : type})

    def runTests(self):
        self.getTests()
        # perfom the reports
        self.runList(self.reports, "reports")
        # perfom the glite reports
        self.runList(self.glite_tests, "glite")
        # perfom the critical tests
        self.runList(self.critical_tests, "critical")

        self.writeResultsPage()

def main():
    tr = TestRunner()
    tr.runTests()

if __name__ == '__main__':
    sys.exit(main())
