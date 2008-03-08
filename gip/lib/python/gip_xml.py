
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


