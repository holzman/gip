#!/usr/bin/env python

import os
import sys
import ConfigParser

class TestRunner:
    def __init__(self, base_path):
        self.base = base_path
        self.cp = ConfigParser.ConfigParser()
        self.cp.readfp(open(base_path + "/etc/tests.conf"))

    def writeResultsPage(self, output_files):
        from test_common import fileOverWrite
        from template import getPageHeader, getPageFooter
        pageHeader = getPageHeader()
        pageFooter = getPageFooter()
        body = "<h1>GIP Testing Results</h1>"

        for file in output_files:
            fullPathItems = file.split("/")
            file = fullPathItems[len(fullPathItems) - 1]
            display = file.replace("_", " ")[:-5]
            body += "<p><a href='" + file + "'>" + display + "</a></p>"

        contents = pageHeader + body + pageFooter
        output_file = self.cp.get("gip", "results_page")
        fileOverWrite(output_file, contents)

        return output_file

    def write_results(self, test, output):
        from test_common import fileOverWrite
        from template import getPageHeader, getPageFooter
        pageHeader = getPageHeader()
        pageFooter = getPageFooter()
        body = ""

        for line in output:
            body += line

        contents = pageHeader + body + pageFooter
        output_file = self.cp.get("gip", "results_dir") + "/" + test + ".html"
        fileOverWrite(output_file, contents)

        return output_file

    def runTests(self):
        from test_common import ls, runCommand

        output_files = []
        test_prefix = "source " + self.base + "/bin/setup.sh; "
        test_dir = self.base + "/libexec"
        test_list = ls(test_dir)
        for test in test_list:
            cmd = test_prefix + test_dir + "/" + test + " " + self.base
            print >> sys.stderr, "Running %s" % cmd
            output = runCommand(cmd)
            output_file = self.write_results(test, output)
            output_files.append(output_file)
        self.writeResultsPage(output_files)

def main(base_dir):
    tr = TestRunner(base_dir)
    tr.runTests()

if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
