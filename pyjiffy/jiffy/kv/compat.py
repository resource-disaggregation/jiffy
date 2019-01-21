import sys

if sys.version_info[0] < 3:
    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO

    # special unicode handling for python2 to avoid UnicodeDecodeError
    def safe_unicode(obj, *args):
        """ return the unicode representation of obj """
        try:
            return unicode(obj, *args)
        except UnicodeDecodeError:
            # obj is byte string
            ascii_text = str(obj).encode('string_escape')
            return unicode(ascii_text)

    def iteritems(x):
        return x.iteritems()

    def iterkeys(x):
        return x.iterkeys()

    def itervalues(x):
        return x.itervalues()

    def nativestr(x):
        return x if isinstance(x, str) else x.encode('utf-8', 'replace')

    def u(x):
        return x.decode()

    def b(x):
        return x

    def next(x):
        return x.next()

    def byte_to_chr(x):
        return x

    def char_to_byte(x):
        return x

    def bytes_to_str(x):
        return x

    unichr = unichr
    xrange = xrange
    basestring = basestring
    unicode = unicode
    bytes = str
    long = long
else:
    def iteritems(x):
        return iter(x.items())

    def iterkeys(x):
        return iter(x.keys())

    def itervalues(x):
        return iter(x.values())

    def byte_to_chr(x):
        return chr(x)

    def char_to_byte(x):
        return ord(x)

    def nativestr(x):
        return x if isinstance(x, str) else x.decode('utf-8', 'replace')

    def bytes_to_str(x):
        return x.decode()

    def u(x):
        return x

    def b(x):
        return x.encode('latin-1') if not isinstance(x, bytes) else x

    next = next
    unichr = chr
    imap = map
    izip = zip
    xrange = range
    basestring = str
    unicode = str
    safe_unicode = str
    bytes = bytes
    long = int
