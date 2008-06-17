
"""
XML implementation of the GIP output.

The XML version of the GIP output targets newer, webservice-based information
services.  Semantically, GLUE is a well-accepted way to describe the structure
of your site.  Unfortunately, the most common implementation of GLUE uses LDIF,
which is no longer a popular way to pass around structured data.

This is NOT the standard implementation of the GLUE schema in XML!  This is a
custom XML schema which is designed to be easy to be updated and passed around
in a decentralized manner.
"""
import os
import sys

def getXMLTemplate(template, name):
    """
    Return a template from a file.

    @param template: Name of the template file in $GIP_LOCATION/templates.
    @param name: Entry in the template file; for now, this is the first
        entry of the DN.
    @return: Template string
    @raise e: ValueError if it is unable to find the template in the file.
    """
    fp = open(os.path.expandvars("$GIP_LOCATION/templates/%s" % template))
    start_str = "<dn GlueCEUniqueID=\"%s>" % name
    buffer = ''
    recording = False
    for line in fp:
        if line.startswith(start_str):
            recording = True
        if recording:
            buffer += line
            if line == '\n':
                break
    if not recording:
        raise ValueError("Unable to find %s in template %s" % (name, template))
    return buffer[:-1]

def printXMLTemplate(template, info):
    """
    Print out the XML contained in template using the values from the
    dictionary `info`.

    The different entries of the template are matched up to keys in the `info`
    dictionary; the entries' values are the dictionary values.

    To see what keys `info` needs for your template, read the template as
    found in::

        $GIP_LOCATION/templates

    @param info: Dictionary of information to fill out for the template.
        The keys correspond to the blank entries in the template string.
    @type info: Dictionary
    @param template: Template string returned from getTemplate.
    """
    print template % info
