
import re

import gip_sections
sec = gip_sections.site

from gip_common import cp_get, voList, getLogger

log = getLogger("GIP.Site")

split_re = re.compile('\s*;?,?\s*')
def filter_sponsor(cp, text):
    vo_list = voList(cp)
    vo_map = dict([(i.lower(), i) for i in vo_list])
    text = text.replace('"', '').replace("'", '')
    entries = split_re.split(text)
    results = []
    if len(entries) == 1:
        entry = entries[0]
        if len(entry.split(':')) == 1:
            entries[0] = entry.strip() + ":100"
    for entry in entries:
        try:
            vo, number = entry.split(":")
            number = float(number)
        except:
            log.warning("Entry for sponsor, `%s`, is not in <vo>:<value>" \
                "format." % str(entry))
            continue
        if vo in vo_map:
            vo = vo_map[vo]
        elif vo.startswith('us') and vo[2:] in vo_map:
            vo = vo_map[vo[2:]]
        elif vo.lower().startswith("local") or \
                vo.lower().startswith("unknown"):
            pass # Do not log warning in this case.
        else:
            log.warning("VO named `%s` does not match any VO in" \
                " osg-user-vo-map.txt." % str(vo))
        results.append("%s:%i" % (vo.lower(), int(number)))
    return " ".join(results)

def generateGlueSite(cp):
    """
    Function which generates the necessary information for a GLUE site entry.
    """

    info = {}
    info['siteName'] = cp_get(cp, sec, "name", "UNKNOWN")
    info['uniqueID'] = cp_get(cp, sec, "unique_name", info['siteName'])
    info['emailContact'] = cp_get(cp, sec, "email", "UNKNOWN@example.com")
    info['contact'] = cp_get(cp, sec, "contact", "UNKNOWN admin")
    info['location'] = '%s, %s' % (cp_get(cp, sec, "city", "UNKNOWN city"),
        cp_get(cp, sec, "country", "UNKNOWN country"))
    info['latitude'] = cp_get(cp, sec, 'latitude', "0.00")
    info['longitude'] = cp_get(cp, sec, 'longitude', "0.00")
    info['website'] = cp_get(cp, sec, 'sitepolicy', "http://example.com/" \
        "site_policy")
    # Get the sponsor, then 
    info['sponsor'] = cp_get(cp, sec, 'sponsor', "UNKNOWN:100")
    info['sponsor'] = filter_sponsor(cp, info['sponsor'])
    info['sponsor'] = '\n'.join(['GlueSiteSponsor: %s' % i for i in \
        info['sponsor'].split()])
    return info

