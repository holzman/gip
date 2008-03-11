
import gip_sections
sec = gip_sections.site

def generateGlueSite(cp):
    """
    Function which generates the necessary information for a GLUE site entry.
    """

    info = {}
    info['uniqueID'] = cp.get(sec, "unique_name")
    info['siteName'] = cp.get(sec, "name")
    info['emailContact'] = cp.get(sec, "email")
    info['contact'] = cp.get(sec, "contact")
    info['location'] = '%s, %s' % (cp.get(sec, "city"), cp.get(sec, "country"))
    info['latitude'] = cp.get(sec, 'latitude')
    info['longitude'] = cp.get(sec, 'longitude')
    info['website'] = cp.get(sec, 'sitepolicy')
    info['sponsor'] = cp.get(sec, 'sponsor')
    return info

