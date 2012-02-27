#!/usr/bin/python

"""
This provider adds the "top-level" GLUE objects to the GIP output.
"""

ldif_top_str = \
"""
dn: o=grid
objectClass: top
objectClass: GlueTop
objectClass: organization
o: grid

dn: mds-vo-name=local,o=grid
objectClass: GlueTop
objectClass: MDS
objectClass: top
Mds-Vo-name: local
"""

print ldif_top_str

