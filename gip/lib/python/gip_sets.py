import sys

py23 = sys.version_info[0] == 2 and sys.version_info[1] >= 3
"""
True if the current version of Python is 2.3 or higher; enables a few extra
capabilities which Python 2.2 does not have.
"""

if not py23:
    from sets24 import *
    from sets24 import _TemporarilyImmutableSet
else:
    import warnings
    warnings.filterwarnings('ignore', category=DeprecationWarning)
    from sets import *
    from sets import _TemporarilyImmutableSet


