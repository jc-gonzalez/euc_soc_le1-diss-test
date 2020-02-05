#!/usr/bin/env python

__version__ = 'Revision: 0.5'

import asyncore, errno, hashlib, os, fnmatch, pickle, signal, socket, stat, string, sys, time, uuid, datetime
import codecs, ssl
import multiprocessing as mp
import base64



try:
    from globals import VT
    VT[__file__] = __version__
except:
    pass

from pdb import set_trace as breakpoint


if hasattr(sys.version_info,'major') and sys.version_info.major > 2:
    from http.client import HTTPSConnection, HTTPConnection
    from urllib.parse import urlparse
    from urllib.parse import urlencode
    from urllib.parse import quote
    from urllib.request import url2pathname
    from urllib.request import urlopen
    import http.cookies as Cookie
    import xmlrpc.client as xmlrpclib
    from itertools import zip_longest as izip_longest
else:
    from httplib import HTTPSConnection, HTTPConnection
    from urlparse import urlparse
    from urllib import urlencode
    from urllib import url2pathname
    from urllib import urlopen
    from urllib import quote
    import Cookie
    import xmlrpclib
    from itertools import izip_longest
try:
    from easdss.config.Environment import Env
except:
    Env = os.environ
try:
    from easdss.log.Message import Message as message
except:
    def message(s):
        """
        substitute message with print
        """
        machine_name = socket.gethostname()
        s_add = "%s %s " % (time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name)
        print(s_add + s)


DSSserver_current = {
              'SDC-NL':'dh-node08.hpc.rug.nl:18443',
              'SDC-FR':'cceuclid1.in2p3.fr:443',
              'SDC-ES':'services01.euclid.pic.es:443',
              'SDC-IT':'euclid-dss.oats.inaf.it:8443',
              'SDC-DE':'eucgu50.euc.mpcdf.mpg.de:8443',
              'SDC-CH':'pieclddss00.isdc.unige.ch:8443',
              'SDC-UK':'euclid-dss.roe.ac.uk:443',
              'SDC-FI':'86.50.169.30:443',
              'SDC-FR-DEV':'cceuclid2.in2p3.fr:8008',
              'SOC':'euclidsoc.esac.esa.int:443',
              }

DSSserver_test = {
              'SDC-NL':'application14.target.rug.nl:18443',
              'SDC-FR':'cceuclidial.in2p3.fr:8008',
              'SDC-IT':'140.105.72.205:443',
              'SDC-FI':'86.50.169.30:443',
              'SDC-CH':'pieclddss00.isdc.unige.ch:8443',
              'SDC-UK':'euclid-dss.roe.ac.uk:443',
              'SDC-DE':'eucgu50.euc.mpcdf.mpg.de:8443',
              'SDC-ES':'services01.euclid.pic.es:443',
              'SDC-US':'dss.ipac.caltech.edu:443',
              'SOC':'euclidsoc.esac.esa.int:443',
             }

DSSserver_dev = {
              'SDC-NL':'application11.target.rug.nl:18443',
              'SDC-FR':'',
              'SDC-IT':'',
              }

DSSserver_pip = {
              'SDC-NL':'dh-node09.hpc.rug.nl:38443',
              }

DSSserver={
           'current':DSSserver_current,
           'test':DSSserver_test,
           'dev':DSSserver_dev,
           'pip':DSSserver_pip,
           }

Ingestserver = {
                 'current':'https://%s:%s@eas-dps-mis.euclid.astro.rug.nl',
                 'test':'https://%s:%s@eas-dps-mis.test.euclid.astro.rug.nl',
                 'dev':'https://%s:%s@eas-dps-mis.dev.euclid.astro.rug.nl',
                 'pip':'https://%s:%s@eas-dps-mis.pip.euclid.astro.rug.nl',
                 }

Cusserver = {
                 'current':'https://eas-dps-cus.euclid.astro.rug.nl',
                 'test':'http://eas-dps-cus.test.euclid.astro.rug.nl',
                 'dev':'http://eas-dps-cus.dev.euclid.astro.rug.nl',
                 'pip':'https://eas-dps-cus.pip.euclid.astro.rug.nl',
                 }

SDCLIST = []

connection_list = {}


from xml.parsers import expat

from xml.sax.saxutils import escape


class Element(object):

    ''' A parsed XML element '''

    def __init__(self, name, attributes):
        '''Record tagname and attributes dictionary'''
        self.name = name
        self.attributes = attributes
        # Initialize the element's cdata and children to empty
        self.cdata = ''
        self.children = []

    def __str__(self):
        ''' convert element and children to string '''
        return self.toString()

    def addChild(self, element):
        '''Add a reference to a child element'''
        self.children.append(element)

    def getAttribute(self, key):
        '''Get an attribute value'''
        return self.attributes.get(key)

    def setAttribute(self, key, value):
        ''' Set an attribute value '''
        self.attributes[key] = value

    def getData(self):
        '''Get the cdata'''
        return self.cdata

    def setData(self, data):
        ''' Set the data '''
        self.cdata = data

    def hasElement(self, name):
        ''' return True if there is a child with given name,
            otherwise False '''
        for child in self.children:
            if child.name == name :
                return True
        return False

    def hasElementLike(self, name):
        ''' return True if there is a child which contains name,
            otherwise False '''
        for child in self.children:
            if child.name.find(name) >- 1 :
                return True
        return False

    def getElement(self, name):
        '''Get the element with name, if a list present return the first '''
        for child in self.children :
            if child.name == name :
                return child
        return None

    def getElements(self, name=''):
        '''Get a list of child elements'''
        if name:
            # return only those children with a matching tag name
            return [c for c in self.children if c.name == name]
        else:
            # no tag name is specified, return the all children
            return list(self.children)

    def getAllElements(self, element_array, name=''):
        ''' Get an array of all elements recursively '''
        if name:
            if self.name == name:
                 element_array.append(self)
        else:
            element_array.append(self)
        for c in self.children:
            c.getAllElements(element_array,name)

    def toString(self, level=0, indent=' '):
        ''' print element and children '''
        indent = indent * level
        retval = indent + "<%s" % self.name
        for attribute in self.attributes:
            retval += ' %s="%s"' % (attribute, self.attributes[attribute])
        content = ""
        for child in self.children:
            content += child.toString(level=level+1)
        if not content and not self.cdata :
            retval += "/>\n"
        else :
            # TODO formatting of output
            #retval += ">%s%s</%s>" % (escape(self.cdata), content, self.name)
            if content :
                content = "\n" + content + indent
            if self.cdata :
                content = '%s%s' % (self.cdata, content)
            retval += ">" + content + ("</%s>\n" % self.name)
        return retval

class Xml2Object(object):

    ''' XML to Object converter '''

    def __init__(self, encoding='', strip_data=True):
        ''' initialize '''
        self.root = None
        self.nodeStack = []
        self.encoding = encoding
        self.strip_data = strip_data

    def StartElement(self, name, attributes):
        '''Expat start element event handler'''
        # Instantiate an Element object
        if self.encoding :
            element = Element(name.encode(self.encoding), attributes)
        else:
            element = Element(name, attributes)
        # Push element onto the stack and make it a child of parent
        if self.nodeStack:
            parent = self.nodeStack[-1]
            parent.addChild(element)
        else:
            self.root = element
        self.nodeStack.append(element)

    def EndElement(self, name):
        '''Expat end element event handler'''
        self.nodeStack.pop()

    def CharacterData(self, data):
        '''Expat character data event handler'''
        if not self.strip_data or data.strip():
            element = self.nodeStack[-1]
            if self.encoding :
                element.cdata += data.encode(self.encoding)
            else :
                element.cdata += data

    def Parse(self, filename, xml=None):
        ''' Create an Expat parser, input can be a filename or the xml
        '''
        Parser = expat.ParserCreate()
        # Set the Expat event handlers to our methods
        Parser.StartElementHandler  = self.StartElement
        Parser.EndElementHandler    = self.EndElement
        Parser.CharacterDataHandler = self.CharacterData
        # Parse the XML File
        if filename :
            xml = open(filename).read()
            if self.encoding :
                xml = xml.decode(self.encoding)
        ParserStatus = Parser.Parse(xml, 1)
        return self.root







def gpgencrypt(lines, defaultkey, fout=None, lout=[], gpghomedir=None):
    '''
    gpgencrypt encrypts lines with defaultkey and writes them to
    file object fout. lines should NOT contain newlines!
    '''
    if gpghomedir == None:
        pcmd = subprocess.Popen('gpg                          --output - --default-key "%(defaultkey)s" --clearsign' % vars(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    else:
        pcmd = subprocess.Popen('gpg --homedir %(gpghomedir)s --output - --default-key "%(defaultkey)s" --clearsign' % vars(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if sys.version_info.major > 2:
        (outtext, errtext) = pcmd.communicate(input=str.encode('\n'.join(lines)))
        outtext = outtext.decode()
        errtext = errtext.decode()
    else:
        (outtext, errtext) = pcmd.communicate(input='\n'.join(lines))
    returncode = pcmd.returncode
    if returncode == None:
        machine_name = socket.gethostname()
        print('%s %s gpgencrypt: kill child!' % (time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name))
        pcmd.kill()
        returncode = -255
    elif returncode:
        if errtext:
            machine_name = socket.gethostname()
            print('%s %s gpgencrypt: %s' % (time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name, errtext))
    elif fout:
        fout.write(outtext)
    else:
        returncode = None
        for l in outtext.split('\n'):
            lout.append(l+'\n')
    return returncode

def gpgdecrypt(fin=None, lin=[], gpghomedir=None):
    '''
    gpgdecrypt decrypts the encrypted lines read from file object
    fin. It returns a list of strings without newlines!
    '''
    r = []
    if fin:
        buf = ''.join(fin.readlines())
    else:
        buf = ''.join(lin)
    if gpghomedir == None:
        pcmd = subprocess.Popen('gpg                          --output - --decrypt' % vars(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    else:
        pcmd = subprocess.Popen('gpg --homedir %(gpghomedir)s --output - --decrypt' % vars(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    if sys.version_info.major > 2:
        (outtext, errtext) = pcmd.communicate(input=str.encode(buf))
        outtext = outtext.decode()
        errtext = errtext.decode()
    else:
        (outtext, errtext) = pcmd.communicate(input=buf)
    returncode = pcmd.returncode
    if pcmd.returncode == None:
        machine_name = socket.gethostname()
        print('%s %s gpgdecrypt: kill child!' % (time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name))
        pcmd.kill()
        returncode = -255
    elif returncode:
        if errtext:
            machine_name = socket.gethostname()
            print('%s %s gpgdecrypt: %s' % (time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name, errtext))
    else:
        r = outtext.split()
    return r

from pdb import set_trace as breakpoint


# extensions for decompressed, gzip and bzip2 compressed
Compressed_Exts = [
    '',
#    '.gz',
#    '.bz2',
]
Compressors = {
    '':         '',
#    '.gz':      'gzip --to-stdout --no-name ',
#    '.bz2':     'bzip2 --stdout --compress ',
}
Decompressors = {
    '':         '',
#    '.gz':      'gzip --to-stdout --decompress ',
#    '.bz2':     'bzip2 --stdout --decompress ',
}

MapAction = {
    'DSSGET': 'GET',
    'DSSSTORE':  'POST',
    'DSSSTORED':  'GET',
    'DSSMAKELOCAL': 'GET',
    'DSSMAKELOCALASY': 'GET',
    'DSSGETLOG': 'GET',
    'GETLOCALSTATS': 'GET',
    'GETGROUPSTATS': 'GET',
    'GETTOTALSTATS': 'GET',
    'LOCATE': 'GET',
    'LOCATEFILE': 'GET',
    'LOCATELOCAL': 'GET',
    'LOCATEREMOTE': 'GET',
    'MD5SUM': 'GET',
    'PING': 'GET',
    'SIZE': 'GET',
    'STAT': 'GET',
    'GET': 'GET',
    'GETANY': 'GET',
    'GETEXACT': 'GET',
    'GETLOCAL': 'GET',
    'GETREMOTE': 'GET',
    'STORE': 'POST',
    'STORED': 'POST',
    'HEAD': 'GET',
    'HEADLOCAL': 'GET',
    'DELETE': 'GET',
    'TAKEOVER': 'GET',
    'TESTCACHE': 'GET',
    'TESTSTORE': 'GET',
    'TESTFILE': 'GET',
    'REGISTER': 'GET',
    'RELEASE': 'GET',
    'RELOAD': 'GET',
    'DSSTESTGET': 'GET',
    'DSSTESTCONNECTION': 'GET',
    'DSSTESTNETWORK': 'GET',
    'DSSINIT': 'GET',
    'DSSINITFORCE': 'GET',
    'DSSGETTICKET': 'GET',
}


# DSS server, username, validity, ticket
dssaccessfile = []

# Trick(y)
if not hasattr(os.path, 'sep'):
    os.path.sep = '/'
    machine_name = socket.gethostname()
    print('%s %s client: Warning: Use at least python version 2.3' % (
        time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name))


def python_to_ascii(*args):
    ''' binary to ascii '''
    return binascii.b2a_hex(pickle.dumps(args))


def ascii_to_python(arg):
    ''' ascii to binary '''
    return pickle.loads(binascii.a2b_hex(arg))


def read_access_file(filename=''):
    """ read file with DSS tickets
    """
    if not filename:
        filename = os.path.join(os.environ['HOME'], '.dssaccess')
    if os.path.isfile(filename):
        f = open(filename, 'r')
        for line in f.readlines():
            tockens = line.split(',')
            if len(tockens) == 4:
                tockens[3]=float(tockens[3])
                dssaccessfile.append(tockens)
        f.close()


def write_access_file(filename=''):
    """ write file with DSS tickets
    """
    if not filename:
        filename = os.path.join(os.environ['HOME'], '.dssaccess')
    f = open(filename, 'w')
    for line in dssaccessfile:
        line[3]=str(line[3])
        f.write(','.join(k for k in line) + '\n')
    f.close()


def find_ticket(server, username):
    """ find ticket for server and user
    """
    tnow = time.time()
    for line in dssaccessfile:
        if line[0] == server and line[1] == username:
            try:
                if float(line[3]) > tnow:
                    return line[2]
                else:
                    machine_name = socket.gethostname()
                    print('%s %s find_ticket: Ticket for %s expired, repeat dssinit' % (
                        time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name, server))
            except:
                return None
    return None

def add_ticket(server, username, ticket, validity):
    """ find ticket for server and user
    """
    for line in dssaccessfile:
        if line[0] == server and line[1] == username:
            line[3] = validity
            line[2] = ticket
            return 1
    line=[server, username, ticket, validity]
    dssaccessfile.append(line)
    return -1

def delete_ticket(server, username):
    """ remove ticket for server and user
    """
    for line in dssaccessfile:
        if line[0] == server and line[1] == username:
            dssaccessfile.remove(line)
    return 1



def delete_tickets():
    """ delete expired tickets
    """
    tnow = (datetime.datetime.utcnow()-datetime.datetime(1970,1,1)).total_seconds()
    machine_name = socket.gethostname()
    for line in dssaccessfile:
        if float(line[3]) < tnow:
            dssaccessfile.remove(line)
            print('%s %s delete_tickets: Ticket for %s expired, repeat dssinit' % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), machine_name, line[0]))


def _ping(host, port):
    """
    See if there is a server running out there
    """
    r = True
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
    except Exception:
        r = False
    s.close()
    return r


class DataIO(object):
    '''
    Class to provide data transfer between the server and client.
    '''
    _host = ''
    _port = 0
    _secure = True
    error_message = ''
    cookie ={}

    def myprint(self, level, text):
        self.textbuf.append(text)
        if level and not (self.debug & level):
            return
        if not self.silent:
            message(text)

    def removesecure(self):
        printsendheader = self.sendheader.copy()
        if 'password' in printsendheader:
            printsendheader['password'] = 'XXX'
        if 'authorization' in printsendheader:
            printsendheader['authorization'] = 'Authorization provided'
        if 'Authorization' in printsendheader:
            printsendheader['Authorization'] = 'Authorization provided'
        return printsendheader

    def __del__(self):
        #        if self.status and self.textbuf:
        #            for text in self.textbuf:
        #                message('DATA_IO: '+text)
        if hasattr(self,'open') and self.open:
            self.f.close()
            self.open = 0

    def __init__(self, host, **kwargs):
        port = kwargs.get('port', None)
        store_host = kwargs.get('store_host', None)
        store_port = kwargs.get('store_port', None)
        debug = kwargs.get('debug', 0)
        timeout = kwargs.get('timeout', None)
        data_path = kwargs.get('data_path', '')
        user_id = kwargs.get('user_id', '')
        sleep = kwargs.get('sleep', 30.0)
        dstid = kwargs.get('dstid', '')
        secure = kwargs.get('secure', True)
        certfile = kwargs.get('certfile', '')
        standard = kwargs.get('standard', False)
        looptime = kwargs.get('looptime', 1.0)
        logfile = kwargs.get('logfile', '')
        username = kwargs.get('username', '')
        password = kwargs.get('password', '')
        accessfile = kwargs.get('accessfile', '')
        cookie = kwargs.get('cookie', {})
        silent = kwargs.get('silent', False)
        # Use standard (i.e. defined by more important persons) HTTP methods
        self.machine_name = socket.gethostname()
        self.keepalive = True
        self.silent = silent
        self.standard = standard
        self.globalname = ''
        self.globalnames = []
        self.textbuf = []
        if dstid:
            self.dstid = dstid
        else:
            self.dstid = '%s' % (uuid.uuid4())
        self.version = __version__.split()[1]
        self.open = 0
        self.result = 0
        self.host_init = host
        if type(host) == type(()):
            self.host = host[0]
            self.port = host[1]
        elif port:
            self.host = host
            self.port = int(port)
        else:
            self.host, port = host.split(':')
            self.port = int(port)
        self.original_host = self.host
        self.allhosts = []
        for h in self.original_host.split(','):
            self.allhosts += socket.gethostbyname_ex(h)[2]
        self.host2 = ''
        self.port2 = 0
        self.timeout = timeout
        self.store_port = self.port
        self.store_host = self.host
        if store_port:
            self.store_port = int(store_port)
            if store_host:
                self.store_host = store_host
        elif store_host:
            hp = store_host.split(':')
            if len(hp) > 1:
                self.store_port = int(hp[1])
        self.f_extra = None
        self.server_id = ''
        self.server_caps = ''
        self.status = 0
        self.debug = debug
        self.autoclose = True
        self.buffer = ''
        self.buffer_length = 0
        self.read_buffer_size = 16 * 1024
        self.write_buffer_size = 16 * 1024
        self.action = "NONE"
        self.bcount = 0
        self.getpath = None
        self.cmd = None
        self.content_length = None
        self.content_type = ''
        if data_path and os.path.isdir(data_path):
            self.data_path = data_path
        else:
            self.data_path = None
        self.myprint(255, '__init__: self.data_path = %s' % (self.data_path))
        self.storecheck = False
        self.storekey = None
        if user_id:
            self.user_id = user_id
        elif 'database_user' in Env:
            self.user_id = Env['database_user']
        else:
            try :
                import pwd
                self.user_id = pwd.getpwuid(os.getuid())[0]
            except:
                self.user_id = '?'
        self.machine_id = socket.getfqdn()
        self.chksum = False
        self.check_md5sum = None
        self.check_sha1 = None
        self.sleep = sleep
        self.certfile = certfile
        if certfile:
            self.secure = True
        else:
            self.certfile = None
            self.secure = secure
        self.secure2 = self.secure
        self.secure3 = None
        self.store_secure = self.secure
        self.sendheader = {}
        self.recvheader = {}
        self.nextsend = False
        self.response = None
        self.sslcontext = None
        self.conn = None
        self.f = None
        self.url = None
        self.handle_handle = None
        self.username = username
        self.password = password
        self.authorization = ''
        self.looptime = looptime
        self.logfile = logfile
        self.client_cookie = {}
        if cookie:
            cookie['secure'] = True
            self.cookie = cookie.copy()
        try:
            self.sslcontext = ssl._create_unverified_context()
        except:
            self.sslcontext = None
            # Just to get the behaviour of older python versions

    def _set_data_path(self, data_path=None):
        if not data_path and 'data_path' in Env and Env['data_path']:
            data_path = Env['data_path']
        if data_path and os.path.isdir(data_path):
            self.data_path = data_path

    def _show(self):

        current_time = time.strftime('%B %d %H:%M:%S', time.localtime())

        print('%s %s _show: self.version             : %s' % (current_time, self.machine_name, self.version))
        print('%s %s _show: self.open                : %d' % (current_time, self.machine_name, self.open))
        print('%s %s _show: self.result              : %d' % (current_time, self.machine_name, self.result))
        print('%s %s _show: self.host                : %s' % (current_time, self.machine_name, self.host))
        print('%s %s _show: self.port                : %d' % (current_time, self.machine_name, self.port))
        print('%s %s _show: self.secure              : %d' % (current_time, self.machine_name, self.secure))
        print('%s %s _show: self.host2               : %s' % (current_time, self.machine_name, self.host2))
        print('%s %s _show: self.port2               : %d' % (current_time, self.machine_name, self.port2))
        print('%s %s _show: self.secure2             : %d' % (current_time, self.machine_name, self.secure2))
        print('%s %s _show: self.timeout             : %f' % (current_time, self.machine_name, self.timeout))
        print('%s %s _show: self.store_host          : %s' % (current_time, self.machine_name, self.store_host))
        print('%s %s _show: self.store_port          : %d' % (current_time, self.machine_name, self.store_port))
        print('%s %s _show: self.store_secure        : %d' % (current_time, self.machine_name, self.store_secure))
        print('%s %s _show: self.f_extra             : %d' % (current_time, self.machine_name, self.f_extra))
        print('%s %s _show: self.server_id           : %s' % (current_time, self.machine_name, self.server_id))
        print('%s %s _show: self.server_caps         : %s' % (current_time, self.machine_name, self.server_caps))
        print('%s %s _show: self.status              : %d' % (current_time, self.machine_name, self.status))
        print('%s %s _show: self.debug               : %d' % (current_time, self.machine_name, self.debug))
        print('%s %s _show: self.autoclose           : %d' % (current_time, self.machine_name, self.autoclose))
        print('%s %s _show: self.buffer              : %s' % (current_time, self.machine_name, self.buffer))
        print('%s %s _show: self.buffer_length       : %d' % (current_time, self.machine_name, self.buffer_length))
        print('%s %s _show: self.read_buffer_size    : %d' % (current_time, self.machine_name, self.read_buffer_size))
        print('%s %s _show: self.write_buffer_size   : %d' % (current_time, self.machine_name, self.write_buffer_size))
        print('%s %s _show: self.recvheader          : %s' % (current_time, self.machine_name, self.recvheader))
        print('%s %s _show: self.sendheader          : %s' % (current_time, self.machine_name, self.removesecure()))
        print('%s %s _show: self.action              : %s' % (current_time, self.machine_name, self.action))
        print('%s %s _show: self.bcount              : %d' % (current_time, self.machine_name, self.bcount))
        print('%s %s _show: self.getpath             : %s' % (current_time, self.machine_name, self.getpath))
        print('%s %s _show: self.content_length      : %d' % (current_time, self.machine_name, self.content_length))
        print('%s %s _show: self.data_path           : %s' % (current_time, self.machine_name, self.data_path))
        print('%s %s _show: self.user_id             : %s' % (current_time, self.machine_name, self.user_id))
        print('%s %s _show: self.machine_id          : %s' % (current_time, self.machine_name, self.machine_id))

    def _connect(self, host, port, secure):

        self.myprint(255, '_connect: %d %d %d %s %s' % (
            self.open, self.result, self.status, self.action, self.dstid))
        self.keepalive = self.keepalive and (host == self._host and port == self._port and secure == self._secure)
        self.bcount = 0
        self.buffer = ''
        self.buffer_length = 0
        self.result = 0
        self.status = 0
        self.recvheader = {}
        self.response = None
        securetry = False
        if self.action not in list(MapAction):
            raise Exception('%s %s _connect: no such action %s' % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self.action))
        if secure:
            securetry = True
            if not self.keepalive:
                if self.sslcontext:
                    self.conn = HTTPSConnection(host, port, cert_file=self.certfile, timeout=self.timeout, context=self.sslcontext)
                else:
                    self.conn = HTTPSConnection(host, port, cert_file=self.certfile, timeout=self.timeout)
                self.myprint(255, '_connect: self.conn = %s' % (self.conn))
            try:
                if self.standard:
                    self.conn.request('POST', self.url, headers=self.sendheader)
                else:
                    self.conn.request(MapAction[self.action], self.url, headers=self.sendheader)
            except Exception as e:
                self.myprint(255, '_connect: error in secure request to %s:%d %s' % (host, port, e))
                if self.conn and hasattr(self.conn,'close'):
                    self.conn.close()
                self._host = ''
                self._port = 0
                secure = False
                self.keepalive = False
        self.secure3 = secure
        if not secure:
            if not self.keepalive:
                self.conn = HTTPConnection(host, port, timeout=self.timeout)
                self.myprint(255, '_connect: self.conn = %s' % (self.conn))
            try:
                if self.standard:
                    self.conn.request('POST', self.url, headers=self.sendheader)
                else:
                    self.conn.request(MapAction[self.action], self.url, headers=self.sendheader)
            except Exception as e:
                self.result = 1
                self.myprint(255, '_connect: error in request to %s:%d %s' % (host, port, e))
                if self.conn and hasattr(self.conn,'close'):
                    self.conn.close()
                self._host = ''
                self._port = 0
                self.keepalive = False
        if not secure and self.result and not securetry:
            securetry = True
            if not self.keepalive:
                if self.sslcontext:
                    self.conn = HTTPSConnection(host, port, cert_file=self.certfile, timeout=self.timeout, context=self.sslcontext)
                else:
                    self.conn = HTTPSConnection(host, port, cert_file=self.certfile, timeout=self.timeout)
                self.myprint(255, '_connect: self.conn = %s' % (self.conn))
            try:
                self.secure3 = secure = True
                if self.standard:
                    self.conn.request('POST', self.url, headers=self.sendheader)
                else:
                    self.conn.request(MapAction[self.action], self.url, headers=self.sendheader)
            except Exception as e:
                self.myprint(255, '_connect: error in secure request to %s:%d %s' % (host, port, e))
                if self.conn and hasattr(self.conn,'close'):
                    self.conn.close()
                self._host = ''
                self._port = 0
                self.result = 1
                self.keepalive = False
        if not self.result:
            if not self.keepalive:
                self._host = host
                self._port = port
                self._secure = secure
            nextsendwas = self.nextsend
            self.myprint(255, '_connect: sendheader = %s, chksum = %s, nextsend = %s' % (self.removesecure(), self.chksum, self.nextsend))
            if self.nextsend:
                self.nextsend = False
                rcount = 0
                try:
                    if self.chksum:
                        self.check_md5sum = hashlib.md5()
                    data = self.f.read(self.write_buffer_size)
                    while data:
                        self.bcount += len(data)
                        if self.chksum:
                            self.check_md5sum.update(data)
                        rcount += 1
                        while data:
                            sent = self.conn.sock.send(data)
                            data = data[sent:]
                        data = self.f.read(self.write_buffer_size)
                    self.f.close()
                    self.open = 0
                    if self.chksum:
                        self.check_md5sum = self.check_md5sum.hexdigest()
                        self.chksum = False
                except Exception as e:
                    self.myprint(255, '_connect: rcount = %(rcount)d, exception = %(e)s' % vars())
                    self.result = 1
            try:
                self.response = self.conn.getresponse()
                self.keepalive = not self.response.will_close
            except Exception as e:
                if self.response and hasattr(self.response, 'close'):
                    self.response.close()
                if self.conn and hasattr(self.conn, 'close'):
                    self.conn.close()
                self.myprint(0, '_connect: error in response from %s:%d (%s)' % (host, port, e))
                self.nextsend= nextsendwas
                self.result = 1
                self._host = ''
                self._port = 0


    def _respond(self):
        def respond_200_get():
            if self.open == 3:
                self.myprint(1, '_respond: writing headers for original client')
                self.handle_handle.send_response(200)
                if self.content_type:
                    self.handle_handle.send_header("Content-Type", self.content_type)
                if self.content_length:
                    self.handle_handle.send_header("Content-Length", self.content_length)
                if self.client_cookie:
                    expires = ''
                    if 'expires' in self.cookie:
                        expires = self.cookie['expires']
                    for k,v in self.client_cookie.items():
                        if k not in ['expires']:
                            cookie_string = '%s=%s;expires=%s;' % (k,v,expires)
                            self.handle_handle.send_header("Set-Cookie", cookie_string)
                self.handle_handle.end_headers()
                self.open = 1
            data = self.response.read(self.read_buffer_size)
            self.myprint(64, '__respond: initially got %d bytes' % (len(data)))
            while not self.result and data:
                if self.content_length and (self.bcount + len(data)) > self.content_length:
                    # this should never occur, since self.response.read takes automatically care of this
                    self.myprint(64, '_respond: got %d, needed %d!' % (self.bcount, self.content_length))
                    self.result = -2
                    break
                if self.open:
                    try:
                        self.f.write(data)
                    except Exception as e:
                        if not self.f_extra:
                            self.result = -1
                        self.open = 0
                        self.myprint(255, '_respond: Exception "%s:' % (e))
                        try:
                            self.myprint(255, '_respond: '+traceback.format_exc())
                        except:
                            self.myprint(255, '_respond: No traceback possible!')
                        self.myprint(255, '_respond: Connection with client broken!')
                if self.f_extra:
                    self.f_extra.write(data)
                self.bcount += len(data)
                self.myprint(128, '_respond: writing : %d' % len(data))
                if self.buffer_length:
                    self.buffer = self.buffer + data
                data = self.response.read(self.read_buffer_size)
                if data == None:
                    self.myprint(128, '_respond: got None!')
                    self.result = -2
                elif len(data):
                    self.myprint(128, '_respond: got %d bytes!' % (len(data)))
                else:
                    self.myprint(128, '_respond: zero bytes!')
                    if not self.result and self.content_length and self.bcount < self.content_length:
                        self.myprint(64, '_respond: got %d, needed %d!' % (self.bcount, self.content_length))
                        self.result = -2
            if not self.open and self.f_extra and not self.result:
                self.result = -1
            if self.response and hasattr(self.response, 'close'):
                self.response.close()

        get_actions = ["GETLOCALSTATS", "GETGROUPSTATS", "GETTOTALSTATS", "LOCATE", "LOCATEFILE", "LOCATELOCAL",
                       "LOCATEREMOTE", "MD5SUM", "PING", "SHA1SUM", "SIZE", "STAT", "DSSTESTGET", "DSSTESTCONNECTION",
                       "DSSTESTNETWORK", "DSSMAKELOCAL", "DSSMAKELOCALASY", "DSSINIT", "DSSINITFORCE", "DSSGETTICKET",
                       "DELETE",]
        self.myprint(255, '_respond: %d %d %d %s %s' % (self.open, self.result, self.status, self.action, self.dstid))
        if self.result:
            if self.conn and hasattr(self.conn, 'close'):
                self.conn.close()
            return
        self.status = self.response.status
        self.recvheader = {}
        for k, v in dict(self.response.getheaders()).items():
            self.recvheader[k.lower()] = v
        self.reason = self.response.reason
        self.myprint(255, '_respond: recvheader = %s' %self.recvheader)
        self.myprint(255, '_respond: status = %d' % self.status)
        self.myprint(255, '_respond: reason = %s' % self.reason)
        if 'content-type' in self.recvheader:
            self.content_type = self.recvheader['content-type']
        if 'content-length' in self.recvheader:
            self.content_length = int(self.recvheader['content-length'])
        else:
            self.content_length = None
        if 'set-cookie' in self.recvheader:
            cookie_items = []
            for t_res in self.response.getheaders():
                if len(t_res)==2:
                    if t_res[0].lower()=='set-cookie':
                        cookie_item = t_res[1].replace(";,",";").split(";")
                        cookie_items.extend(cookie_item)
            for item in cookie_items:
                if item:
                    item_p = item.split("=")
                    if len(item_p)==2:
                        self.cookie[item_p[0].strip()] = item_p[1].strip()
                    elif len(item_p)==1 and item_p[0]:
                        self.cookie[item_p[0].strip()]
        if self.status == 200:
            self.result = 0
            if self.action in get_actions:
                if self.content_length == None:
                    raise Exception('server did not specify "Content-Length"!')
                self.buffer_length = self.content_length
                self.buffer = self.response.read(self.buffer_length)
                while len(self.buffer) < self.content_length:
                    self.buffer += self.response.read(self.buffer_length - len(self.buffer))
                if type(b'') != type(''):
                    self.buffer = str(self.buffer, "utf-8")
                if self.response and hasattr(self.response, 'close'):
                    self.response.close()
            elif self.action in ["STORE", "DSSSTORE"]:
                self.nextsend = True
                if 'storecheck' in self.recvheader and 'storekey' in self.recvheader:
                    self.storecheck = self.recvheader['storecheck'] == 'OK'
                    self.storekey = self.recvheader['storekey']
                    self.chksum = True
                self.response.fp.close()
            elif self.action in ["GET", "GETANY", "GETEXACT", "GETLOCAL", "GETLOG", "GETREMOTE", "DSSGET"]:
                if 'content-length' not in self.recvheader:
                    self.myprint(255, '_respond: no content-length in recvheader')
                    self.result = -2
                else:
                    respond_200_get()
            elif self.action in ["HEAD", "HEADLOCAL"]:
                self.result = 0
                #self.response.close()
        elif self.status == 201:
            self.open = 2
        elif self.status == 204:
            self.result = 0
            if self.action in ["STORE", "STORED", "DSSSTORE", "DSSSTORED"]:
                self.open = 2
                if 'datapath' in self.recvheader and 'storekey' in self.recvheader:
                    self.storecheck = True
                    self.storekey = self.recvheader['storekey']
                    datapath = os.path.join(self.data_path, self.recvheader['datapath'])
                    self.check_md5sum = hashlib.md5()
                    self.check_sha1 = hashlib.sha1()
                    fdatapath = open(datapath, 'ab')
                    if not fdatapath:
                        self.result = -1
                    if not self.result:
                        data = self.f.read(self.write_buffer_size)
                        while data:
                            self.bcount += len(data)
                            self.check_md5sum.update(data)
                            self.check_sha1.update(data)
                            fdatapath.write(data)
                            data = self.f.read(self.write_buffer_size)
                        fdatapath.close()
                    self.check_md5sum = self.check_md5sum.hexdigest()
                    self.check_sha1 = self.check_sha1.hexdigest()
                    self.open = 1
            elif 'link-name' in self.recvheader:
                linkname = os.path.join(self.data_path, 'xdata', self.recvheader['link-name'])
                localname = self.f.name
                self.f.close()
                self.f = None
                self.open = 0
                os.remove(localname)
                os.symlink(linkname, localname)
        elif self.status in [301, 302, 303, 307]:
            if 'location' in self.recvheader:
                if self.status in [301, 302, 307]:
                    self.result = 2
                elif self.status in [303]:
                    self.result = 3
                redirhost = urlparse(self.recvheader['location'])
                self.myprint(255, '_respond: redirhost = %s status =  %d' % (redirhost, self.status))
                h, p = redirhost[1].split(':')
                self.host2 = h
                if p == '':
                    self.port2 = 80
                else:
                    self.port2 = int(p)
                if redirhost[0] == 'https':
                    self.secure2 = True
                else:
                    self.secure2 = False
                if self.status == 301:
                    self.store_port = self.port2
                    self.store_host = self.host2
                if self.status == 303:
                    inext = ''
                    ouext = ''
                    self.cmd = None
                    for ext in Compressed_Exts:
                        if ext and redirhost[2].endswith(ext):
                            inext = ext
                        if ext and self.getpath.endswith(ext):
                            ouext = ext
                    self.getpath = url2pathname(redirhost[2])
                    if inext and not ouext:
                        self.cmd = Decompressors[inext]
                    elif not inext and ouext:
                        self.cmd = Compressors[ouext]
                    elif inext and ouext:
                        self.cmd = Decompressors[inext] + '| ' + Compressors[ouext]
                    self.myprint(255, '_respond: cmd = %s getpath = %s' % (self.cmd, self.getpath))
            else:
                self.result = 1
            self.myprint(255, '_respond: result = %d status = %d' % (self.result, self.status))
        elif self.status in [400, 404, 501]:
            self.result = 1
        elif self.status == 503:
            self.result = 1
            self.myprint(0, '_respond: Server too busy to handle request! [dstid = %s]' % (self.dstid))
            self.response.fp.close()
            sleeptime = 60.0
            if 'sleep-time' in self.recvheader['sleep-time']:
                time.sleep(float(self.recvheader['sleep-time']))
            return
        else:
            self.result = 1
            self.myprint(255, '_respond: result = %d status = %d' % (self.result, self.status))
#        self.response.fp.close()
#        if self.response and hasattr(self.response, 'close'): self.response.close()
#        if self.conn and hasattr(self.conn, 'close'): self.conn.close()
        if self.open == 2 or self.nextsend:
            self.myprint(255, '_respond: self.open = %s self.nextsend = %s' % (self.open, self.nextsend))
        elif self.open:
            if not self.autoclose:
                self.open = 0
            elif not self.result or self.cmd:
                self.f.close()
                self.open = 0


    def _setup(self, action, path, **kw):
        '''
        sets up the connection string and connection
        '''
        host = kw.get('host', None)
        port = kw.get('port', None)
        secure = kw.get('secure', None)
        query = kw.get('query', None)
        fileuri = kw.get('fileURI', '')
        jobid = kw.get('jobid', '')
        use_data_path = kw.get('use_data_path', True)
        defaulthp = self.host == None and self.port == None
        if not host:
            host = self.host
        if not port:
            port = self.port
        self.secure3 = None
        if not secure:
            secure = self.secure
        self.myprint(255, '_setup: host:port = %s:%d' % (host, port))
        self.action = action
        if action in ["STORE","DSSSTORE"]:
            head, path = os.path.split(path)
        if action != "DELETE" and os.path.isabs(path):
            path = path[len(os.path.sep):]
        if action.startswith("GET"):
            self.getpath = path
        rest = ''
        self.chksum = False
        self.sendheader = {}
        if kw:
            for k, v in kw.items():
                if k not in ['host', 'port', 'secure', 'query', 'use_data_path']:
                    if type(v) == type(b''):
                        v = v.decode()
                    self.sendheader[k] = '%s' % (v)
        self.sendheader['Author'] = 'K.G. Begeman'
        self.sendheader['Client'] = 'httplib'
        self.sendheader['Client-Version'] = '%s' % (self.version)
        self.sendheader['DSTID'] = '%s' % (self.dstid)
        if self.recvheader and 'ProxyHost' in self.recvheader:
            hp_r = self.recvheader['ProxyHost'].split(':')
            if len(hp_r) == 2:
                h = hp_r[0]
                p = int(hp_r[1])
#            elif len(hp_r)==1:
#                h=hp_r[0]
#                p=int(80)
            else:
                h = hp_r[0]
                p = int(80)
            self.sendheader['Host'] = '%s:%d' % (h, p)
            host = h
            port = p
        else:
            if self.host and not self.host[0].isdigit():
                self.sendheader['Host'] = '%s:%d' % (self.host, port)
            else:
#                self.sendheader['Host'] = '%s:%d' % (socket.getfqdn(host), port)
                self.sendheader['Host'] = '%s:%d' % (self.host, port)
        self.sendheader['TimeStamp'] = '%s' % (time.strftime('%Y-%m-%dT%H:%M:%S'))
        self.sendheader['Action'] = action
        self.sendheader['pragma'] = action
#        print('DataIO cookie:',self.cookie)
        nocookie = True
        if self.cookie and 'server' in self.cookie and self.cookie['server']==self.host_init and action not in ['DSSINIT','DSSINITFORCE']:
            cookie_string = ";".join(["{0}={1}".format(k, v) for k, v in self.cookie.items()])
            self.sendheader['Cookie'] = '%s' % (cookie_string)
            nocookie = False
        if nocookie and self.username and self.password:
            self.sendheader['Authorization'] = 'Basic %s' % (base64.b64encode(b"%s:%s" % (self.username.encode('utf-8'), self.password.encode('utf-8'))).decode('utf-8'))
        if self.standard:
            queryaction = urlencode({'ACTION':action})
            if query:
                rest = '?%s&%s' % (queryaction, query)
            else:
                rest = '?%s' % (queryaction)
        elif query:
            rest = '?%s' % (query)
        if action in ["STORE", "DSSSTORE"]:
            if self.nextsend:
                self.chksum = True
                self.sendheader['RECEIVE'] = 'OK'
            else:
                self.sendheader['RECEIVE'] = 'NOT OK'
            self.sendheader['StoreCheck'] = 'OK'
        if action in ["STORE", "DSSSTORE"] and self.open:
            if self.sendheader['RECEIVE']=='OK':
                self.content_length = os.fstat(self.f.fileno())[6]
                self.sendheader['Content-Length'] = '%d' % (self.content_length)
            else:
                self.sendheader['Content-Length'] = '0'
        else:
            self.content_length = 0
        if self.data_path and use_data_path:
            self.sendheader['Data-Path'] = '%s' % (self.data_path)
        if action in ["STORED", "DSSSTORED"] and self.storekey:
            self.nextsend = False
            self.sendheader['StoreKey'] = '%s' % (self.storekey)
            # if self.check_md5sum:
            self.sendheader['MD5SUM'] = '%s' % (self.check_md5sum)
            self.sendheader['SHA1'] = '%s' % (self.check_sha1)
            self.sendheader['BCOUNT'] = '%s' % (self.bcount)
        if self.user_id:
            self.sendheader['User-ID'] = '%s' % (self.user_id)
        if self.machine_id:
            self.sendheader['Machine-ID'] = '%s' % (self.machine_id)
        if fileuri:
            self.sendheader['URI'] = '%s' % (fileuri)
        if action == "DSSGETLOG" and jobid:
            self.sendheader['JOBID'] = '%s' % (jobid)
        self.url = '/%s%s' % (path, rest)
        self.keepalive = self.keepalive and (self._host and self._port) and defaulthp
        if not self.keepalive:
            hosts = list(set(socket.gethostbyname_ex(host)[2]))
            # get hosts and remove duplicates
            self.myprint(255, '_setup: hosts = %s' % hosts)
            if len(hosts) > 1:
                hosts.sort()
                l = len(hosts)
                shift = random.randrange(0, l)
                hs = [hosts[(shift+i)%l] for i in range(l)]
                hosts = hs
            for h in hosts:
                self.status = 503
                while self.status == 503:
                    self._connect(h, port, secure)
                    if not self.result:
                        self._respond()
                    else:
                        break
                    #if self.status == 503: time.sleep(60.0)
                if self.result in [0, 2, 3]:
                    break
        else:
            self._connect(self._host, self._port, self._secure)
            if not self.result:
                self._respond()


    def delete(self, path='', savepath=None, fd=None):
        '''
        delete removes a file from a server, i.e. it moves the file to the
        ddata directory on the servers disk. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''
        def deleteError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s delete: Error deleting file on %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        self._setup("DELETE", path, host=self.host, port=self.port)
        r = ''
        if self.result:
            deleteError()
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
            return True
        else:
            r = ''
            deleteError()
        return False



    def getlog(self, path='', defaultkey='', host=None, port=None, gpghomedir=None):
        '''
        getlog retrieves the log file from the server.

        path    = name of the file the logdata will be store in.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        text = []
        r = gpgencrypt(['GETLOG'], defaultkey, fout=None, lout=text, gpghomedir=gpghomedir)
        if r:
            return False
        self.autoclose = True
        self.f = open(path + '_INCOMPLETE', "wb")
        if self.f:
            self.open = 1
            ptext = python_to_ascii(text)
            self._setup("GETLOG", path, host=host, port=port, Validation=ptext, use_data_path=False)
            if self.open:
                self.f.close()
                self.open = 0
            if self.result:
                if os.path.exists(path + '_INCOMPLETE'):
                    os.remove(path + '_INCOMPLETE')
            else:
                if os.path.exists(path + '_INCOMPLETE'):
                    os.rename(path + '_INCOMPLETE', path)
                return True
        return False

    def restart(self, defaultkey='', host=None, port=None, gpghomedir=None):
        text = []
        r = gpgencrypt(['RESTART'], defaultkey, fout=None, lout=text, gpghomedir=gpghomedir)
        if r:
            return False
        ptext = python_to_ascii(text)
        self._setup("RESTART", "restart", host=host, port=port, Validation=ptext)
        if self.result:
            r = False
        else:
            r = True
        return r

    def md5sum(self, path='', host=None, port=None):
        '''
        md5sum calculates the md5 check sum of a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("MD5SUM", path, host=host, port=port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        else:
            r = ''
        return r

    def sha1sum(self, path='', host=None, port=None):
        '''
        md5sum calculates the sha1 check sum of a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("SHA1SUM", path, host=host, port=port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        else:
            r = ''
        return r

    def size(self, path='', host=None, port=None):
        '''
        size calculates the size of a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("SIZE", path, host=host, port=port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        else:
            r = ''
        return r

    def stat(self, path='', host=None, port=None):
        '''
        stat returns the stat tuple a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("STAT", path, host=host, port=port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = pickle.loads(url2pathname(self.buffer[:self.buffer_length]))
        else:
            r = ()
        return r

    def getstats(self, mode='LOCAL'):
        '''
        get local stats
        '''
        self.open = 0
        path = 'dummy'
        if mode in ['GLOBAL', 'TOTAL']:
            self._setup("GETTOTALSTATS", path, host=self.host, port=self.port)
        elif mode == 'GROUP':
            self._setup("GETGROUPSTATS", path, host=self.host, port=self.port)
        else:
            self._setup("GETLOCALSTATS", path, host=self.host, port=self.port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length].split('<>')
        else:
            r = []
        return r

    def _locateit(self, path, local=False, remote=False):

        self.open = 0
        if local and remote:
            self._setup("LOCATE", path, host=self.host, port=self.port)
        elif not local and not remote:
            self._setup("LOCATEFILE", path, host=self.host, port=self.port)
        elif local and not remote:
            self._setup("LOCATELOCAL", path, host=self.host, port=self.port)
        elif not local and remote:
            self._setup("LOCATEREMOTE", path, host=self.host, port=self.port)
        if self.result:
            if self.status == 404:
                r = []
            else:
                r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length].split('<>')
        else:
            r = []
        return r

    def locate(self, path=''):
        '''
        locate locates a file, i.e. returns the url of the server
        where the specified file resides. All data servers are probed
        for the file.

        path    = name of the file
        '''

        return self._locateit(path, local=True, remote=True)

    def locatefile(self, path=''):
        '''
        locatelocal locates a file, i.e. returns the url of the server
        where the specified file resides. Only the addressed dataserver
        is probed.

        path    = name of the file
        '''

        return self._locateit(path, local=False, remote=False)

    def locatelocal(self, path=''):
        '''
        locatelocal locates a file, i.e. returns the url of the server
        where the specified file resides. Only the local dataservers
        are probed.

        path    = name of the file
        '''

        return self._locateit(path, local=True, remote=False)

    def locateremote(self, path=''):
        '''
        locateremote locates a file, i.e. returns the url of the server
        where the specified file resides. Only remote dataservers are
        probed.

        path    = name of the file
        '''

        return self._locateit(path, local=False, remote=True)

    def testfile(self, path=''):
        '''
        testfile tests whether a file exists on the server

        path    = name of the file
        '''
        self.open = 0
        self._setup("TESTFILE", path, host=self.host, port=self.port)
        if self.result == 0 and self.status == 204:
            return True
        else:
            return False

    def testcache(self):
        '''
        testcache tests whether the server is allowed to cache files
        '''
        self.open = 0
        self._setup("TESTCACHE", 'testcache', host=self.host, port=self.port)
        if self.result == 0:
            return True
        else:
            return False

    def teststore(self):
        '''
        teststore tests whether the server is allowed to store files
        '''
        self.open = 0
        self._setup("TESTSTORE", 'teststore', host=self.host, port=self.port)
        if self.result == 0 and self.status == 204:
            return True
        else:
            return False

    def _getit(self, **kwargs):
        '''
        _getit handles the retrieving of files from local or remote servers

        path    = name of the file
        local   = local server (True) or only remote (False)
        remote  = remote server (True) or local server (False)
        handle  = handle to the DataRequestHandler
        extra   = extra fd to write to
        query   = extra url query
        savepath= name of file where to store the retrieved data [name of originalfile]
        exact   = exact location of file is given/wanted
        '''
        path = kwargs.get('path', None)
        fd = kwargs.get('fd', None)
        local = kwargs.get('local', False)
        remote = kwargs.get('remote', True)
        exact = kwargs.get('exact', False)
        handle = kwargs.get('handle', None)
        extra = kwargs.get('extra', None)
        raise_exception = kwargs.get('raise_exception', False)
        query = kwargs.get('query', None)
        savepath = kwargs.get('savepath', None)

#        print("path:",path)
        kw = {}

        for kwkey, kwvalue in kwargs.items():
            if kwkey not in ['path']:
                kw[kwkey] = kwvalue

        results = []
        if type(path) == type([]):
            paths = path
        else:
            paths = [path]
        for thispath in paths:
            if exact:
                action = "GETEXACT"
            elif local:
                if remote:
                    if handle or fd or query:
                        action = "GET"
                    else:
                        action = "GETANY"
                else:
                    action = "GETLOCAL"
            else:
                action = "GETREMOTE"
            action="DSSGET"
            use_data_path = False
            if extra:
                self.f_extra = extra
            if handle:
                self.autoclose = False
                self.f = handle.wfile
                self.handle_handle = handle
                self.open = 3
                self._setup(action, thispath, **kw)
                self.myprint(255, '_getit: result = %d' % self.result)
                while self.result > 1 and not exact:
                    self.open = 3
                    self._setup(action, thispath, **kw)
                    self.myprint(255, '_getit: result = %d' % self.result)
                self.handle_handle = None
                self.open = 0
                lpath = None
            else:
                if fd:
                    self.autoclose = False
                    if hasattr(fd, 'name'):
                        lpath = fd.name
                    else:
                        lpath = None
                    self.f = fd
                else:
                    self.autoclose = True
                    if savepath:
                        lpath = savepath
                    else:
                        head, tail = os.path.split(thispath)
                        if tail == '':
                            lpath = head
                        else:
                            lpath = tail
                    if not os.path.exists(lpath):
                        self.f = open(lpath + '_INCOMPLETE', "wb")
                        self.myprint(255, '_getit: OPEN ' + lpath + '_INCOMPLETE')
                        if not extra:
                            use_data_path = True
                    else:
                        self.f = None
                if self.f:
                    self.open = 1
                    self._setup(action, thispath, **kw)
                    self.myprint(255, '_getit: _setup result = %d' % self.result)
                    if self.result == 0 and self.status == 204 and use_data_path:
                        self.autoclose = True
                    self.myprint(255, '_getit: result = %d' % self.result)
                    while self.result > 1 and not exact:
                        if self.open:
                            self.f.seek(0)
                            self.f.truncate()
                        elif self.autoclose and lpath:
                            if self.result == 3:
                                self.cmd = subprocess.Popen(shlex.split(self.cmd), stdin=subprocess.PIPE, stdout=open(lpath + '_INCOMPLETE', "wb"))
                                self.f = self.cmd.stdin
                                self.myprint(255, '_getit: POPEN %s > %s' % (self.cmd, lpath + '_INCOMPLETE'))
                            else:
                                self.f = open(lpath + '_INCOMPLETE', "wb")
                        self.open = 1
                        if self.result == 3:
                            self._setup("GET", self.getpath, **kw)
                        else:
                            self._setup(action, thispath, **kw)
                        self.myprint(255, '_getit: result = %d' % self.result)
                else:
                    self.open = 0
                    self.result = 0
                if self.result:
#                    print(self.result, lpath)
                    self._clear(path=lpath)
                    if raise_exception:
                        raise IOError("%s %s _getit: Error retrieving remote file %s from %s:%d [DSTID=%s]" % (
                            time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, thispath, self._host, self._port, self.dstid))
            if self.open:
                self.f.close()
                self.open = 0
            if self.cmd:
                if hasattr(self.cmd, 'wait'):
                    retcode = self.cmd.wait()
                    if not self.result:
                        self.result = retcode
                self.cmd = None
            if self.result:
                if self.autoclose and lpath and os.path.exists(lpath + '_INCOMPLETE'):
                    self.myprint(255, '_getit: removing "%s"' % (lpath + '_INCOMPLETE'))
                    os.remove(lpath + '_INCOMPLETE')
                self.autoclose = True
                results.append(False)
            else:
                if self.autoclose and lpath and os.path.exists(lpath + '_INCOMPLETE'):
                    self.myprint(255, '_getit: renaming "%s"' % (lpath + '_INCOMPLETE'))
                    os.rename(lpath + '_INCOMPLETE', lpath)
                self.autoclose = True
                results.append(True)
        if len(paths) == 1:
            return results[0]
        return results

    def head(self, path=None, query=None):
        self._setup("HEAD", path, host=self.host, port=self.port, query=query)
        if self.result == 2:
            self._setup("HEAD", path, host=self.host2, port=self.port2, secure=self.secure, query=query)
        if self.result == 0:
            return self.recvheader
        else:
            return None

    def headlocal(self, path=None, query=None):
        self._setup("HEADLOCAL", path, host=self.host, port=self.port, query=query)
        if self.result == 0:
            return self.recvheader
        else:
            return None

    def get(self, **kwargs):
        '''
        get obtains the file from a remote server (see _getit)
        '''
        path = kwargs.get('path', None)
        fd = kwargs.get('fd', None)
        handle = kwargs.get('handle', None)
        extra = kwargs.get('extra', None)
        raise_exception = kwargs.get('raise_exception', True)
        query = kwargs.get('query', None)
        savepath = kwargs.get('savepath', None)
        client_cookie = kwargs.get('client_cookie', {})
        if client_cookie:
            self.client_cookie = client_cookie.copy()
        return self._getit(path=path, fd=fd, local=True, remote=True, exact=False, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath)

    def getlocal(self, **kwargs):
        '''
        getlocal obtains the file from a local server (see _getit)
        '''
        path = kwargs.get('path', None)
        fd = kwargs.get('fd', None)
        handle = kwargs.get('handle', None)
        extra = kwargs.get('extra', None)
        raise_exception = kwargs.get('raise_exception', False)
        query = kwargs.get('query', None)
        savepath = kwargs.get('savepath', None)
        return self._getit(path=path, fd=fd, local=True, remote=False, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath)

    def getremote(self, **kwargs):
        '''
        getlocal obtains the file from a local server (see _getit)
        '''
        path = kwargs.get('path', None)
        fd = kwargs.get('fd', None)
        handle = kwargs.get('handle', None)
        extra = kwargs.get('extra', None)
        raise_exception = kwargs.get('raise_exception', False)
        query = kwargs.get('query', None)
        savepath = kwargs.get('savepath', None)
        return self._getit(path=path, fd=fd, remote=False, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath)

    def getexact(self, **kwargs):
        '''
        getlocal obtains the file from a local server (see _getit)
        '''
        path = kwargs.get('path', None)
        fd = kwargs.get('fd', None)
        handle = kwargs.get('handle', None)
        extra = kwargs.get('extra', None)
        raise_exception = kwargs.get('raise_exception', False)
        query = kwargs.get('query', None)
        savepath = kwargs.get('savepath', None)
        return self._getit(path=path, fd=fd, remote=False, exact=True, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath)

    def register(self, path='', validation=''):
        '''
        register gets a dataserver to register a file which is already
        in one of the dataserver directories.

        path    = path to the file (w.r.t. the working directory)
        '''
        self.open = 0
        self._setup("REGISTER", path, host=self.host, port=self.port, Validation=validation)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False

    def release(self, validation=''):
        '''
        release gets a dataserver to release the port the server is listening on
        '''
        self.open = 0
        self._setup("RELEASE", '/dummy', host=self.host, port=self.port, Validation=validation)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False

    def takeover(self, validation='', newport='', certfile=''):
        '''
        takeover gets a dataserver to take over the port the current server is listening on
        '''
        if not certfile:
            certfile = self.certfile
        self.open = 0
        query = ''
        if certfile:
            query = urlencode({'NEWPORT': newport, 'CERTFILE': certfile})
        else:
            query = urlencode({'NEWPPORT': newport})
        self._setup("TAKEOVER", '/dummy', host=self.host, port=self.port, Validation=validation, Newport=newport, certfile=certfile)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False


    def cachefile(self, path=''):
        '''
        cachefile requests a caching server to cache a file

        path    = name of the file
        '''
        self.open = 0
        self._setup("CACHEFILE", path, host=self.host, port=self.port)
        if self.result == 0 and self.status == 204:
            return True
        else:
            return False


    def mirrorput(self, path='', port=8000, directory=''):
        '''
        mirrorput make the data server retrieve file path to its sdata directory

        path        = path to local file
        port        = port local ds listens on
        directory   = change the directory where the file should be mirrored
        '''
        fs = os.stat(path)
        query = urlencode({'INFO': pickle.dumps((fs[stat.ST_ATIME], fs[stat.ST_MTIME], port, directory), protocol=2)})
        self._setup("MIRRORSTORE", path, query=query)
        if self.result:
            self.buffer_length = 0
            return False
        else:
            self.buffer_length = 0
            return True

    def getcaps(self):
        '''
        Get the capability string from server.
        '''
        if self.server_id == '' and self.server_caps == '':
            self.ping()
        return self.server_caps

    def getid(self):
        '''
        Get the server id.
        '''
        if self.server_id == '' and self.server_caps == '':
            self.ping()
        return self.server_id

    def ping(self):
        '''
        See if there is a server running out there and get the server info.
        '''
        curtime = time.time()
        self._setup("PING", 'ping', host=self.host, port=self.port)
        if not self.result:
            self.server_ping_time = time.time() - curtime
        if self.result:
            self.server_id = ''
            self.server_caps = ''
            self.server_ping_time = 0.0
            self.buffer_length = 0
            return False
        else:
            self.server_id = ''
            self.server_caps = ''
            self.buffer_length = 0
            split_buffer = self.buffer.split()
            if len(split_buffer) >= 1:
                self.server_id = split_buffer[0]
            if len(split_buffer) >= 2:
                self.server_caps = split_buffer[1]
            if len(split_buffer) < 1:
                self.result = 1
                return False
            else:
                return True

    def _clear(self, path=''):
        '''
        Clean up after problems with retrieving.
        '''
        if self.open:
            self.f.close()
        self.open = 0
        if path and os.path.exists(path):
            os.remove(path)
        return self.result

    def checksum(self, path=''):
        '''
        checksum locates a file and returns for each result the path, host, port and checksum
        '''
        r = []
        ir = self.locate(path)
        for i in ir:
            d = dataserver_result_to_dict(i)
            if d['path'] != '?':
                if 'md5sum' in d.keys():
                    r.append((d['ip'], d['port'], d['path'], d['md5sum']))
                else:
                    r.append((d['ip'], d['port'], d['path'], self.md5sum(d['path'], d['ip'], int(d['port']))))
        return r

    def dssinit(self):
        """ Initialize localpassword
        """
        def testError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s dssinit: Error testing connection to %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        self.open = 0
        self._setup("DSSINIT", 'dssinit', host=self.host, port=self.port)
        r = ''
        if self.result:
            testError()
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        elif 'set-cookie' in self.recvheader:
            return True
        else:
            r = ''
            testError()
        return False

    def dssinitforce(self):
        """ Initialize localpassword
        """
        def testError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s dssinitforce: Error testing connection to %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        self.open = 0
        self._setup("DSSINITFORCE", 'dssinitforce', host=self.host, port=self.port)
        r = ''
        if self.result:
            testError()
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        elif 'set-cookie' in self.recvheader:
            return True
        else:
            r = ''
            testError()
        return False

    def dssgetticket(self):
        """ Initialize localpassword
        """
        def testError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s dssgetticket: Error testing connection to %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        self.open = 0
        self._setup("DSSGETTICKET", 'dssgetticket', host=self.host, port=self.port)
        r = ''
        if self.result:
            testError()
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        elif 'set-cookie' in self.recvheader:
            return True
        else:
            r = ''
            testError()
        return False




    def dsstestget(self):
        '''
        dsstestget returns 1 kb string
        '''
        def testError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s dsstestget: Error testing connection to %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        self.open = 0
        self._setup("DSSTESTGET", 'dsstestget', host=self.host, port=self.port)
        r = ''
        if self.result:
            testError()
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        else:
            r = ''
            testError()
        if len(r) == 1024:
            return True
        return False

    def dsstestconnection(self):
        '''
        dsstestnetwork returns dictionary of time required to connect to DSS servers
        '''
        def testError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s dsstestconnection: Error testing connection to %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))
        self.open = 0
        self._setup("DSSTESTCONNECTION", 'dsstestconnection', host=self.host, port=self.port)
        r = ''
        if self.buffer_length and not self.result:
            r = self.buffer[:self.buffer_length]
        else:
            testError()
        return r

    def dsstestnetwork(self):
        '''
        dsstestnetwork returns dictionary of time required to exchange 1 kb string between DSS servers
        '''
        def testError():
            """
            print out an error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s dsstestnetwork: Error testing connection to %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))
        self.open = 0
        self._setup("DSSTESTNETWORK", 'dsstestnetwork', host=self.host, port=self.port)
        r = ''
        if self.buffer_length and not self.result:
            r = self.buffer[:self.buffer_length]
        else:
            testError()
        return r

    def makelocal(self, path='', savepath=None, fd=None):
        '''
        cachefile requests a caching server to cache a file

        path    = name of the file
        '''

        def cacheError():
            """
            publish error
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s makelocal: Error storing file %s on %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, path, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        def jobstatuscycle():
            """
            cycle to check status of job
            """
            cycle_counter = 0
            return_code = False
            cycle_break = False
            while True:
                time.sleep(self.looptime)
                self._setup("DSSGETLOG", '', host=self.host, port=self.port, jobid=self.jobid)
                if self.result == 0 and self.status == 200:
                    if 'jobstatus' in self.recvheader:
                        self.jobstatus = self.recvheader['jobstatus']
                        if self.jobstatus == 'RUNNING':
                            cycle_counter += 1
                        elif self.jobstatus == 'FINISHED':
                            return_code = True
                            cycle_break = True
                        elif self.jobstatus == 'FAILED':
                            if 'jobmessage' in self.recvheader:
                                self.jobmsg = self.recvheader['jobmessage']
                                self.error_message = self.jobmsg
                            return_code = False
                            cycle_break = True
                        else:
                            self.error_message = 'Unknown job status %s' % (self.jobstatus)
                            return_code = False
                            cycle_break = True
                    else:
                        self.error_message = 'No jobstatus in returned message'
                        return_code = False
                        cycle_break = True
                else:
                    return_code = False
                    cycle_break = True
                if cycle_break:
                    return return_code

        return_code = False
        fileuri = ''
        if path.find("://") > -1:
            fileuri = path
            path = path.split("/")[-1]
        self.open = 0
        self._setup("DSSMAKELOCAL", path, host=self.host, port=self.port, fileuri=fileuri)
        if self.result:
            cacheError()
            return_code = False
        if self.result == 0 and self.status == 200:
            if 'jobstatus' in self.recvheader and 'jobid' in self.recvheader:
                self.jobstatus = self.recvheader['jobstatus']
                self.jobid = self.recvheader['jobid']
                if self.jobstatus == 'FINISHED':
                    return_code = True
                elif self.jobstatus == 'FAILED':
                    if 'jobmessage' in self.recvheader:
                        self.jobmsg = self.recvheader['jobmessage']
                        self.error_message = self.jobmsg
                    cacheError()
                    return_code = False
                elif self.jobstatus == 'RUNNING':
                    return_code = jobstatuscycle()
                    if not return_code:
                        cacheError()
                else:
                    self.error_message = 'Unknown job status %s' % (self.jobstatus)
                    cacheError()
                    return_code = False
            else:
                self.error_message = 'No jobstatus or jobid in returned header'
                cacheError()
                return_code = False
        else:
            cacheError()
            return_code = False
        return return_code

    def makelocalasy(self, path='', savepath=None, username='', password='', fd=None):
        '''
        makelocalasy requests a caching server to cache a file

        path    = names of the file separated by ;
        '''

        def cacheError():
            """
            Report error during caching
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s makelocalasy: Error storing file %s on %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, path, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        fileuri = None
        if path.find("://") > -1:
            fileuri = path
            path = path.split("/")[-1]

        self.open = 0
        self._setup("DSSMAKELOCALASY", path, host=self.host, port=self.port, fileURI=fileuri)
        if self.result == 0 and self.status == 200 and self.buffer_length:
            r = self.buffer[:self.buffer_length]
            return r
        else:
            cacheError()
            return ''

    def put(self, path='', savepath=None, fd=None):
        '''
        put stores a file on a local dataserver

        path    = path to local file
        '''
        def putError():
            """
            report error for put
            """
            print_recvheader = self.recvheader
            print_sendheader = self.removesecure()
            raise IOError("%s %s put: Error storing file %s on %s:%d [DSTID=%s], internal error %s, recvheader=%s, sendheader=%s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, path, self._host, self._port, self.dstid, self.error_message, print_recvheader, print_sendheader))

        def jobstatuscycle():
            """
            cycle for checking job status
            """
            cycle_counter = 0
            return_code = False
            cycle_break = False
            while True:
                time.sleep(self.looptime)
                self._setup("DSSGETLOG", '', host=self.host, port=self.port, jobid=self.jobid)
                if self.result == 0 and self.status == 200:
                    if 'jobstatus' in self.recvheader:
                        self.jobstatus = self.recvheader['jobstatus']
                        if self.jobstatus == 'FINISHED':
                            return_code = True
                            cycle_break = True
                        elif self.jobstatus == 'FAILED':
                            if 'jobmessage' in self.recvheader:
                                self.jobmsg = self.recvheader['jobmessage']
                            self.error_message = self.jobmsg
                            return_code = False
                            cycle_break = True
                        elif self.jobstatus == 'RUNNING':
                            cycle_counter += 1
                        else:
                            self.error_message = 'Unknown job status %s' % (self.jobstatus)
                            return_code = False
                            cycle_break = True
                    else:
                        self.error_message = 'No jobstatus in response'
                        return_code = False
                        cycle_break = True
                else:
                    self.error_message = 'error returned'
                    return_code = False
                    cycle_break = True
                if cycle_break:
                    return return_code
            return return_code

        return_code = True
        if path.find("://") > -1:
            path = path.split("/")[-1]
        if savepath is None or len(savepath) == 0:
            savepath = path
        if not os.path.exists(savepath):
            raise IOError("%s %s put: Error storing non-existent local file: %s" % (
                time.strftime('%B %d %H:%M:%S', time.localtime()), self.machine_name, path))
        else:
            self.f = open(savepath, "rb")
            self.open = 1
            self._setup("DSSSTORE", path, host=self.store_host, port=self.store_port, use_data_path=self.data_path)
            if self.result == 2:
                self.store_host = self.host2
                self.store_port = self.port2
                self.store_secure = self.secure2
                if not self.open:
                    self.f = open(savepath, "rb")
                    self.open = 1
                self._setup("DSSSTORE", path, host=self.store_host, port=self.store_port,
                            secure=self.store_secure, use_data_path=self.data_path)
            if not self.result and self.nextsend:
                self._setup("DSSSTORE", path, host=self.store_host, port=self.store_port,
                            secure=self.store_secure, use_data_path=False)
            if not self.result and self.storecheck:
                # (self.status == 204 or self.storecheck):
                self._setup("DSSSTORED", path, host=self._host, port=self._port)
                if self.result:
                    message('put: error executing DSSSTORED [result=%d]' % (self.result))
                    putError()
                    return_code = False
                else:
                    if 'jobstatus' in self.recvheader and 'jobid' in self.recvheader:
                        self.jobstatus = self.recvheader['jobstatus']
                        self.jobid = self.recvheader['jobid']
                        if self.jobstatus == 'FINISHED':
                            return_code = True
                        elif self.jobstatus == 'FAILED':
                            if 'jobmessage' in self.recvheader:
                                self.jobmsg = self.recvheader['jobmessage']
                            putError()
                            return_code = False
                        elif self.jobstatus == 'RUNNING':
                            return_code = jobstatuscycle()
                            if not return_code:
                                putError()
                        else:
                            self.error_message = 'Unknown job status %s' % (self.jobstatus)
                            putError()
                            return_code = False
                    else:
                        self.error_message = 'No jobstatus or jobid in response'
                        putError()
                        return_code = False

            elif self.result:
                putError()
                return_code = False
        return return_code


def fillDDO(filenamesarray,parentddotype,parentddoid,targetsdc,purpose):
    FCJR_text='''<FileCopyJobRef>
        <Id>%(fcjrid)s</Id>
        <FileCopyJobStatus>NEW</FileCopyJobStatus>
        <DataFileName>%(filename)s</DataFileName>
    </FileCopyJobRef>'''
    DDO_text='''<?xml version="1.0" encoding="UTF-8"?>
<orc:DDO xmlns:sys="http://euclid.esa.org/schema/sys"
 xmlns:sgs="http://euclid.esa.org/schema/sys/sgs"
 xmlns:dss="http://euclid.esa.org/schema/sys/dss"
 xmlns:tsk="http://euclid.esa.org/schema/sys/tsk"
 xmlns:ial="http://euclid.esa.org/schema/sys/ial"
 xmlns:bas="http://euclid.esa.org/schema/bas"
 xmlns:orc="http://euclid.esa.org/schema/sys/orc"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <Id>%(ddoid)s</Id>
    <ParentType>%(parentddotype)s</ParentType>
    <ParentID>%(parentddoid)s</ParentID>
    <TargetSDC>%(targetsdc)s</TargetSDC>
    <Purpose>%(purpose)s</Purpose>
    <ProcessingState>NEW</ProcessingState>
    %(fcjrarray)s
</orc:DDO>'''
    fcjrarray=''
    for filename in filenamesarray:
        fcjrid='FCJR-'+str(uuid.uuid4())
        fcjrarray+=FCJR_text % vars()+'\n'
    ddoid='DDO-'+str(uuid.uuid4())
    return DDO_text % vars()




def dataserver_result_to_dict(result):
    '''
    method to make dictionary of dataserver locate result
    '''
    d = {}
    for elem in result.split(','):
        key, value = elem.split('=')
        d[key] = value
    return d

def getmd5db(filename, environment):
    if environment not in Cusserver:
        message('Can not find CUS for your environment {}'.format(environment))
        return None
    eas_dps_cus = Cusserver[environment]
    url_md5=eas_dps_cus+"/EuclidXML?class_name=DataContainerStorage&Filename=%(filename)s&PROJECT=DSS" % vars()
    response=urlopen(url_md5)
    inp_xml=response.read()
    s1=inp_xml.find('<CheckSumValue>')
    s2=inp_xml.find('</CheckSumValue>')
    if s1>-1 and s2>-1 and s2>(s1+15):
        return inp_xml[s1+15:s2]
    return None


def messageIAL(command,filename,localfilename,exitcode,errormessage, environment):
    m_type_1='''filename=%s\nfileuri=%s\nexitcode=%s\nmessage=%s\n'''
    m_type_2='''filename=%s\nstatus=%s\nmessage=%s\n'''
    m_type_retrieve='''{"filename":"%s","fileuri":"%s","exitcode":"%s","message":"%s"}'''
    m_type_store='''{"filename":"%s","fileuri":"%s","status":"%s","exitcode":"%s","message":"%s"}'''
    m_type_local='''{"filename":"%s","status":"%s","status_message":"%s","exitcode":"%s","message":"%s"}'''
    m_type_local_asy='''{"exitcode":"%s","message":"%s","result":[%s]
                    }
                      '''
    file_template='''{"filename":"%s","status":"%s","status_message":"%s"}'''
    message_str=''
    if command=='store':
        filename_local=filename.strip().replace("'","")
        if localfilename:
            fileuri=localfilename.strip().replace("'","")
        else:
            fileuri=filename_local
        filename_local=filename_local.split("/")[-1]
        if exitcode:
#           message_str = m_type_1 % (filename, localfilename,0,'')
            message_str=m_type_store % (filename_local,fileuri,"COMPLETED","True","null",)
        else:
            errormessage=errormessage.replace('"','').replace("'","").replace("{","").replace("}","")
            if errormessage.find("already exist")>-1:
                #check MD5
                md5_db = getmd5db(filename_local, environment)
                if md5_db:
                    md5_local = None
                    with open(fileuri) as checkfile:
                        data_local = checkfile.read()
                        md5_local = hashlib.md5(data_local).hexdigest()
                    if md5_db == md5_local:
                        message_str=m_type_store % (filename_local,fileuri,"COMPLETED","True","File already exists in DSS")
                    else:
                        message_str=m_type_store % (filename_local,fileuri,"ERROR","False","File already exists in DSS")
                else:
                    message_str=m_type_store % (filename_local,fileuri,"ERROR","False","Cannot retrieve MD5 from EAS-DPS")
            else:
                message_str=m_type_store % (filename_local,fileuri,"ERROR","False", errormessage)
    elif command=='retrieve':
        filename_local=filename.strip().replace("'","")
        if localfilename:
            fileuri=localfilename.strip().replace("'","")
        else:
            fileuri=filename_local
        filename_local=filename_local.split("/")[-1]
        if exitcode:
#           message_str = m_type_1 % (filename, localfilename,0,'')
            message_str=m_type_retrieve % (filename_local,fileuri,"True","null",)
        else:
            errormessage=errormessage.replace('"','').replace("'","").replace("{","").replace("}","")
            message_str=m_type_retrieve % (filename_local,fileuri,"False", errormessage)
    elif command=='make_local' or command=='make_local_test':
        if exitcode:
            message_str=m_type_local % (filename.strip().replace("'",""),"COMPLETED","null","True","null")
        else:
            errormessage=errormessage.replace('"','').replace("'","").replace("{","").replace("}","")
            message_str=m_type_local % (filename.strip().replace("'",""),"ERROR",'null',"False",errormessage)
    elif command=='make_local_asy':
        if exitcode:
            content=errormessage.replace("u'","").replace("{","").replace("}","").replace("FAILED","ERROR").replace("FINISHED","COMPLETED").replace("RUNNING","EXECUTING").replace("STARTED","EXECUTING")
            files=content.split(",")
            files_content=[]
            try:
                for f_i in files:
                    f1,f2=f_i.split(":")
                    f1=f1.strip().replace("'","")
                    f2=f2.strip().replace("'","")
                    files_content.append(file_template % (f1,f2,'null'))
                message_str=m_type_local_asy % ("True","null",",\n".join(files_content))
            except Exception as e:
                message("ERROR in make_local_asy for file %s" % f_i)
                raise(e)
        else:
            errormessage=errormessage.replace('"','').replace("'","").replace("{","").replace("}","")
            message_str=m_type_local_asy % ("False",errormessage,"")
    elif command=='ping':
        errormessage=errormessage.replace('"','').replace("'","").replace("{","").replace("}","")
        message_str='''{exitcode:"%s",message:"%s"}''' % (exitcode,errormessage)
    elif command in ['dsstestget', 'dsstestconnection', 'dsstestnetwork','dssgetticket','dssinit','dssinitforce']:
        errormessage=errormessage.replace('"','').replace("'","").replace("{","").replace("}","")
        message_str='''{exitcode="%s",message:"%s"}''' % (exitcode,errormessage)
    else:
        message_str='''{exitcode="%s",message:"%s"}''' % ('False','Unknown command')
    message(message_str)
    return


def get_params(args):
    path=args[3]
    localpath=None
    debug=0
    if len(args)==4:
        localpath=args[3]
    elif len(args)==5:
        localpath=args[3]
        try:
            debug = int(args[4])
        except:
            debug=0
            localpath=args[4]
    elif len(args)==6:
        localpath=args[4]
        debug = int(args[5])
    else:
        usage()
        sys.exit(0)
    return path, localpath, debug


def load_xml_file(inp_path):
    if os.path.exists(inp_path):
        tmp_el=Xml2Object().Parse(inp_path)
        filename_els=[]
        filename=[]
        tmp_el.getAllElements(filename_els,name='FileName')
        for i in filename_els:
            newfilename=i.getData()
            if newfilename not in filename:
                filename.append(newfilename)
        return filename
    else:
        message('No input XML file %s' % inp_path)
        sys.exit()
    return []

def connect_string(input_string):
    if input_string.find("http://")>-1:
        input_string=input_string.replace("http://","")
    if input_string.find("https://")>-1:
        input_string=input_string.replace("https://","")
    return input_string


def getlocalmd5(filename):
    length = 16 * 1024
    md5 = hashlib.md5()
    with open(filename, 'rb') as fd:
        for chunk in iter(lambda: fd.read(length), b''):
            md5.update(chunk)
    return md5.hexdigest()


def checkfileexist(filename,localpath=None,ec_environment=None,checkfiles=False):
    try:
        dps_server=Cusserver[ec_environment]
    except:
        message('Can not file CUS for your environment %s' % (ec_environment))
        return False, False
    if localpath and os.path.isfile(localpath):
        md5_local = getlocalmd5(localpath)
    elif filename and os.path.isfile(filename):
        md5_local = getlocalmd5(filename)
    elif checkfiles:
        md5_local = ''
    else:
        return False, False
    url_md5=dps_server+"/EuclidXML?class_name=DataContainerStorage&Filename=%(filename)s&PROJECT=DSS" % vars()
    response=urlopen(url_md5)
    inp_xml=response.read()
    inp_xml=str(inp_xml)
    s1=inp_xml.find('<CheckSumValue>')
    s2=inp_xml.find('</CheckSumValue>')
    if s1>-1 and s2>-1 and s2>(s1+15):
        md5db=inp_xml[s1+15:s2]
        if md5db == md5_local:
            return True, True
        else:
            if not checkfiles:
                message('Error ingesting file - file with such name already exist and has different MD5: %s' % (localpath))
            return True, False
    return False, False


def store_datafile(path,localpath=None,ec_environment=None,SDC=None,username='',password='',debug=0,useoldfiles=False,checkfiles=False,nocert=False):
    def make_local_files(path, username=username, password=password):
        path = path.split("/")[-1] # added to remove directories
        cache_all = True
        for i in list(connection_list):
            try:
                res_cache = True
                result_asy = ''
                t1 = time.time()
                while True:

#                   res_cache = connection_list[i].cachefile(path,username=username,password=password)
                    result_asy = connection_list[i].makelocalasy(path=path)
                    t2 = time.time()
                    if result_asy.find('FINISHED') > -1:
                        message('File %s copied to SDC %s for %.1f sec' % (path,i,t2-t1))
                        break
                    elif result_asy.find('FAILED') > -1:
                        message('Failed to copy file %s to %s' % (path,i))
                        res_cache = False
                        break
                    else:
                        time.sleep(connection_list[i].loop_time)
                        result_asy = ''
            except Exception as e:
                message('Failed to copy file %s to %s' % (path,i))
            if not res_cache:
                cache_all=False
        return cache_all

    if not path:
        message('No file to ingest')
        return False
    if not ec_environment:
        message('No environment specified')
        return False
    if ec_environment not in DSSserver.keys():
        message('Unknown environment %s' % ec_environment)
        return False
    if not SDC:
        message('Unknown SDC')
        return False

    try:
        dss_server=DSSserver[ec_environment][SDC]
    except:
        message('Can not find DSS server in settings for  --environment=%s --SDC=%s' % (ec_environment,SDC))
        return False
    fileexist = False
    makefilelocal = False
    fileexist, makefilelocal = checkfileexist(path,localpath=localpath,ec_environment=ec_environment, checkfiles=checkfiles)
    if fileexist:
        if useoldfiles:
            if makefilelocal:
                message('Data file already exist: %s' % (path))
                if connection_list:
                    res_copy = make_local_files(path,username=username, password=password)
                    if not res_copy:
                        message('Error distributing file %s over SDCs' % (path))
                return True
        elif checkfiles:
            message('Data file with such name already exist: %s' % (path))
            if connection_list:
                res_copy = make_local_files(path,username=username, password=password)
                if not res_copy:
                    message('Error distributing file %s over SDCs' % (path))
            return True
        else:
            message('Error ingesting file - file with such name already exist: %s' % (path))
        return False

    t1=time.time()
    ds_connect = DataIO(connect_string(dss_server), debug=debug,nocert=nocert, username=username, password=password)
    errormes=''
    check_path=path
    if localpath:
        check_path=localpath
    if not os.path.isfile(check_path):
        message('No such local file: %s' % (check_path))
        return False
    try:
        r = ds_connect.put(path=path, savepath=localpath)
    except Exception as errmes:
        errormes=str(errmes)
        if ds_connect.response:
            errormes+=' DSS Server message:'
            errormes+=str(ds_connect.response.reason)
            r = False
            if errormes.find("already exist in the metadata database") and useoldfiles:
                message('Data file already exist: %s' % (path))
                if connection_list:
                    res_copy = make_local_files(path,username=username, password=password)
                    if not res_copy:
                        message('Error distributing file %s over SDCs' % (path))
                return True
        message('Error ingesting data file to DSS server %s: %s  - %s' % (connect_string(dss_server),path,str(errormes)))
        return False
    t2=time.time()
    message('Data file ingested for %.1f sec: %s' % (t2-t1,path))
    if connection_list:
        res_copy = make_local_files(path,username=username, password=password)
        if not res_copy:
            message('Error distributing file %s over SDCs' % (path))
    return True


def store_metadatafile(path,ec_environment=None, project='ALL', username=None, password=None):
    if not path:
        message('No file to ingest')
        return False
    if not username or not password:
        message('Specify username and password')
        return False
    if not ec_environment:
        message('No environment specified')
        return False
    if ec_environment not in Ingestserver.keys():
        message('Unknown environment %s' % ec_environment)
        return False
    ingest_timeout=3600.0
    ingest_server=Ingestserver[ec_environment] % (username,quote(password))
    try:
        sleep_time=1.0
        if os.path.exists(path):
            if os.stat(path).st_size > 1000000:
                sleep_time=60.0
                ingest_timeout=3600.0
            input_string=codecs.open(path,"r","utf-8",'replace').read()
            input_ascii_string=input_string.encode('ascii',errors='replace')
        else:
            input_ascii_string=path
            path='DDO'
        client = None
        sslcontext = None
        try:
            sslcontext = ssl._create_unverified_context()
            client=xmlrpclib.ServerProxy(ingest_server,context=sslcontext)
        except:
            client=xmlrpclib.ServerProxy(ingest_server)
        if not isinstance(client,xmlrpclib.ServerProxy):
            message('Can not create connection with ingest server')
            return False
        res=client.IngestObjectAsy(input_ascii_string,project,True)
        t_start = time.time()
        n_ingest_attempt = 0
        while True:
            time.sleep(sleep_time)
            res2=client.GetLog(res['logfile'])
            if str(res2['result'])=='ok':
                message('XML metadata file ingested for %.1f seconds: %s' % (res2['total_time'],path))
                return True
            elif str(res2['result'])=='FAILED':
                message('Error ingesting XML file: %s - %s' % (path,res2['message']+' '+res2['error']))
                return False
            else:
                if n_ingest_attempt > 2:
                    message('Error ingesting XML file: %s - %s' % (path,' maximum number of attempts exceeded'))
                    return False
                t_check = time.time() - t_start
                if t_check > ingest_timeout:
                    res=client.IngestObjectAsy(input_ascii_string,project,True)
                    t_start=time.time()
                    n_ingest_attempt = n_ingest_attempt +1
    except Exception as errmes:
        message('Error ingesting XML file: %s - %s' % (path,str(errmes)))
    return False


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

def replacenull(inp):
    arr=[]
    for i in inp:
        if i:
            arr.append(i)
    return arr

def chunk(l,n):
    for i in range(0, len(l), n):
        yield l[i:i+n]

def makelocalbatch(filelist, server, username, password, debug, nocert, q, timeout=0):
    import ast
    errorstr = ''
    srv = DataIO(connect_string(server), debug=debug, nocert=nocert, username=username, password=password)
    t1 = time.time()
    while True:
        try:
            res = srv.makelocalasy(path=filelist)
            message("filelist=%s, res=%s" % (filelist, res))
            newfilelist = []
            res_dict = ast.literal_eval(res)
            for k, v in res_dict.items():
                if v.upper() == 'RUNNING':
                    newfilelist.append(k)
                elif v.upper() == 'FAILED':
                    errorstr_i = "%s;" % (k)
                    errorstr = errorstr + errorstr_i
            if len(newfilelist) > 0:
                filelist=";".join(newfilelist)
                if timeout >0 and (time.time() -t1) > timeout:
                    errorstr = errorstr + filelist
                    break
            else:
                break
        except Exception as e:
            q.put(filelist)
            message("Exception in connection to DSS server %s" % str(e))
            break
        time.sleep(10.0)
    q.put(errorstr)
    return

def makelocalsingle(filename, server, username, password, debug, nocert, q, timeout=0):
    import ast
    errorstr = ''
    srv = DataIO(connect_string(server), debug=debug, nocert=nocert, username=username, password=password)
    t1 = time.time()
    message("Starting to make local at %s file %s" % (connect_string(server), filename))
#    try:
#        res = srv.delete(path=filename)
#    except Exception as e:
#        message("Error in deleting local file %s: %s" % (filename, str(e)))
    try:
        res = srv.makelocal(path=filename)
    except Exception as e:
        t2 = time.time()
        message("Error in making local at %s file %s after %s sec: %s" % (connect_string(server), filename, t2-t1, str(e)))
        del srv
        q.put(filename)
        return
    if not res:
        errorstr = filename
    t2 = time.time()
    message("Finished to make local at %s file %s after %s sec" % (connect_string(server), filename, t2-t1))
    del srv
    q.put(errorstr)
    return

def makelocalSDCasy(filelist, server, sdc, n_parallel, username, password, debug, nocert, dumpfile, timeout=0):
    filelist_div = chunk(filelist,n_parallel)
    p_array=[]
    q_array=[]
    for i_f in filelist_div:
        q=mp.Queue()
        i_f_str=";".join(i_f).replace(" ","")
        p=mp.Process(target=makelocalbatch,args=(i_f_str,server,username,password,debug, nocert, q, timeout))
        p.start()
        p_array.append(p)
        q_array.append(q)
    for q_i in q_array:
        q_i_res = q_i.get()
        if len(q_i_res)>0:
            out_files=q_i_res.split(";")
            for o_f in out_files:
                if o_f:
                    dumpfile.write(o_f+"\n")
    for p_i in p_array:
        p_i.join()
    for p_i in p_array:
        if p_i.is_alive():
            p_i.teminate()


def makelocalSDC(filelist, server, sdc, username, password, debug, nocert, dumpfile, timeout=0):
#    filelist_div = chunk(filelist,n_parallel)
    p_array=[]
    q_array=[]
    for i_f in filelist:
        q=mp.Queue()
        p=mp.Process(target=makelocalsingle,args=(i_f,server,username,password,debug, nocert, q, timeout))
        p.start()
        p_array.append(p)
        q_array.append(q)
    for q_i in q_array:
        q_i_res = q_i.get()
        if len(q_i_res)>0:
            out_files=q_i_res.split(";")
            for o_f in out_files:
                if o_f:
                    dumpfile.write(o_f+"\n")
    for p_i in p_array:
        p_i.join()
    for p_i in p_array:
        if p_i.is_alive():
            p_i.teminate()

def main():
    def usage():
        message('''\nUsage: \n %s retrieve inputXMLfile|datafile  [--SDC=SDC_to_retriev_files_from] [--localdirectory=/path/to/store/file] [--environment=Euclid environment] [--noXML] [--project=EAS_project] [--nocert]
 %s store  inputXMLfile|datafile|directory [--SDC=SDC_to_store_files] [--localdirectory=/path/to/local/files] [--environment=Euclid environment] [--SDCLIST=SDC-1,SDC-2,SDC-3] [--noXML] [--username=username] [--password=password or file to path with password] [--createDDO] [--useoldfiles] [--skipfiles] [--checkfiles] [--project=EAS_project] [--nocert]
 %s make_local_asy  inputfilelist  [--environment=Euclid environment] [--SDCLIST=SDC-1,SDC-2,SDC-3] [--username=username] [--password=password or path to file with password] [--nocert] [--nparallel=n] [--nfilesinbatch=n]
 %s make_local  inputfilelist  [--environment=Euclid environment] [--SDCLIST=SDC-1,SDC-2,SDC-3] [--username=username] [--password=password or path to file with password] [--nocert] [--nparallel=n] 
                                              ''' % (os.path.basename(sys.argv[0]),os.path.basename(sys.argv[0]),os.path.basename(sys.argv[0]),os.path.basename(sys.argv[0])))


    debug = 0
    errmes=None
    dss_server=''
    ec_environment=None
    SDC='SDC-NL'
    localpath=None
    path=None
    debug=0
    username=''
    password=''
    useoldfiles=False
    skipfiles=False
    checkfiles=False
    createDDO=False
    noXML=False
    ddo_input=''
    load_metadata=True
    userproject='ALL'
    nocert=False
    localfiles={}
    global SDCLIST
    global connection_list

    for param in sys.argv:
        p_kv=param.split("=")
        if len(p_kv)==2 and p_kv[0]=='--password':
            password = p_kv[1]
            if password and os.path.isfile(password):
                newpass = ''
                with open(password,"r") as f:
                    newpass = f.read()
                password = newpass.replace("\n","").strip()

    if len(sys.argv) < 3 or sys.argv[1] not in ['retrieve', 'store', 'make_local','make_local_asy']:
        usage()
    elif sys.argv[1] == 'retrieve':
        if '--noXML' in sys.argv:
            for param in sys.argv:
                p_kv=param.split("=")
                if len(p_kv)==2:
                    if p_kv[0]=='--environment':
                        ec_environment=p_kv[1]
                    if p_kv[0]=='--SDC':
                        SDC=p_kv[1]
                    if p_kv[0]=='--localdirectory':
                        localpath=p_kv[1]
                    if p_kv[0]=='--username':
                        username=p_kv[1]
                    if p_kv[0]=='--project':
                        userproject=p_kv[1]
                    if p_kv[0]=='--nocert':
                        nocert=p_kv[1]
            try:
                dss_server=DSSserver[ec_environment][SDC]
            except:
                message('no such environment or SDC: --environment=%s --SDC=%s' % (ec_environment,SDC))
                sys.exit()
            ds_connect = DataIO(connect_string(dss_server), debug=debug, nocert=nocert, username=username, password=password)
            path=sys.argv[2]
            path_check=path
            if localpath:
                path_check=os.path.join(localpath,path)
                localpath=path_check
            if os.path.exists(path_check):
                message("Data file already exist on local disk %s" % (path_check))
            else:
                errormes=''
                try:
                    t1=time.time()
                    r = ds_connect.get(path=path, savepath=localpath)
                    t2=time.time()
                    message('Data file retrieved for %.1f sec: %s' % (t2-t1,path_check))
                except Exception as errmes:
                    errormes=str(errmes)
                    if ds_connect.response:
                        errormes+=' DSS Server message:'
                        errormes+=str(ds_connect.response.reason)
                    r = False
                    messageIAL('retrieve',path,localpath,r,errormes, ec_environment)
        else:
            filenames=load_xml_file(sys.argv[2])
            for param in sys.argv:
                p_kv=param.split("=",1)
                if len(p_kv)==2:
                    if p_kv[0]=='--environment':
                        ec_environment=p_kv[1]
                    if p_kv[0]=='--SDC':
                        SDC=p_kv[1]
                    if p_kv[0]=='--localdirectory':
                        localpath=p_kv[1]
                    if p_kv[0]=='--username':
                        username=p_kv[1]
                    if p_kv[0]=='--project':
                        userproject=p_kv[1]
                    if p_kv[0]=='--nocert':
                        nocert=p_kv[1]
            try:
                dss_server=DSSserver[ec_environment][SDC]
            except:
                message('no such environment or SDC: --environment=%s --SDC=%s' % (ec_environment,SDC))
                sys.exit()
            ds_connect = DataIO(connect_string(dss_server), debug=debug,nocert=nocert, username=username, password=password)
            for f_i in filenames:
                errormes=''
                path_check=f_i
                localpath_i=localpath
                if localpath:
                    path_check=os.path.join(localpath,f_i)
                    localpath_i=path_check
                if os.path.exists(path_check):
                    message("Data file already exist on local disk %s" % (path_check))
                else:
                    try:
                        t1=time.time()
                        r = ds_connect.get(path=f_i, savepath=localpath_i)
                        t2=time.time()
                        message('Data file retrieved for %.1f sec: %s' % (t2-t1,path_check))
                    except Exception as errmes:
                        errormes=str(errmes)
                        if ds_connect.response:
                            errormes+=' DSS Server message:'
                            errormes+=str(ds_connect.response.reason)
                        r = False
                        messageIAL('retrieve',f_i,localpath,r,errormes, ec_environment)
                        message('Data file %s not retrieved!' % (f_i))
    elif sys.argv[1] == 'store':
        path=sys.argv[2]
        for param in sys.argv:
            p_kv=param.split("=")
            if len(p_kv)==2:
                if p_kv[0]=='--environment':
                    ec_environment=p_kv[1]
                if p_kv[0]=='--SDC':
                    SDC=p_kv[1]
                if p_kv[0]=='--SDCLIST':
                    tmpSDClist=p_kv[1]
                    try:
                        SDCLIST=tmpSDClist.split(',')
                    except:
                        message('SDC list is not well-formed: %s' % (tmpSDClist))
                        sys.exit()
                if p_kv[0]=='--localdirectory':
                    localpath=p_kv[1]
                if p_kv[0]=='--username':
                    username=p_kv[1]
                if p_kv[0]=='--useoldfiles':
                    useoldfiles=p_kv[1]
                if p_kv[0]=='--skipfiles':
                    skipfiles=p_kv[1]
                if p_kv[0]=='--checkfiles':
                    checkfiles=p_kv[1]
                if p_kv[0]=='--project':
                    userproject=p_kv[1]
                if p_kv[0]=='--nocert':
                    nocert=p_kv[1]
        if '--useoldfiles' in sys.argv:
            useoldfiles=True
        if '--skipfiles' in sys.argv:
            skipfiles=True
        if '--checkfiles' in sys.argv:
            checkfiles=True
        if '--createDDO' in sys.argv:
            createDDO=True
        if '--noXML' in sys.argv:
            noXML=True
        if not ec_environment:
            message('Specify environment')
            sys.exit()
        for i_SDC in SDCLIST:
            if i_SDC not in DSSserver[ec_environment]:
                message('No such SDC: %s' % i_SDC)
                sys.exit()
            elif len(DSSserver[ec_environment][i_SDC])==0:
                message('%s DSS server record for environmnet %s is not specified' % (i_SDC, ec_environment))
                sys.exit()
            else:
                dss_server = DSSserver[ec_environment][i_SDC]
                ds_connect = DataIO(connect_string(dss_server), debug=debug,nocert=nocert,username=username,password=password)
                connection_list[i_SDC] = ds_connect
        if localpath:
            for root, dirnames, inpfilenames in os.walk(localpath):
                for inpfile in inpfilenames:
                    filename = inpfile.split("/")[-1]
                    localfiles[filename]=os.path.join(root,inpfile)
        if os.path.isdir(path):
            parentddotype='TEST'
            parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
            purpose='TEST'
            if not username or not password:
                message('Specify username and password')
                sys.exit()
            for root, dirnames, inpfilenames in os.walk(path):
                message('Processing directory: %s' % (root))
                for xmlfilename in fnmatch.filter(inpfilenames,'*.xml'):
                    load_metadata=True
                    message('Found xml file: %s' % (xmlfilename))
                    filenames=load_xml_file(os.path.join(root,xmlfilename))
                    if skipfiles:
                        message('Skipping following files: %s' % str(filenames))
                    else:
                        for f_i in filenames:
                            datafile=os.path.join(root,f_i)
                            if localpath:
                                datafile=os.path.join(localpath,f_i)
                                if f_i in localfiles:
                                    datafile=localfiles[f_i]
                            if not store_datafile(f_i,datafile,ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,checkfiles=checkfiles,nocert=nocert):
                                load_metadata=False
                                break
                    if not noXML and load_metadata:
                        load_metadata = store_metadatafile(os.path.join(root,xmlfilename),ec_environment, userproject, username, password)
                    else:
                        message('Due to previous errors in ingesting data files metadata file is not ingested: %s ' % xmlfilename)

                    if createDDO and not noXML and not load_metadata:
                        createDDO=False
                        message('Due to errors in ingesting metadata file DDO will not be created for files in %s' % (xmlfilename))

                    if createDDO:
                        for sdc_i in SDCList:
                            if sdc_i != SDC:
                                ddo_input=fillDDO(filenames,parentddotype,parentddoid,sdc_i,purpose)
                                store_metadatafile(ddo_input,ec_environment,'DSS',username,password)
                    if not noXML and not load_metadata:
                        message('Due to previous errors in ingesting data files or metadata metadata file is not ingested: %s ' % xmlfilename)
        elif os.path.isdir(path) and noXML:
            parentddotype='TEST'
            parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
            purpose='TEST'
            if not username or not password:
                message('Specify username and password')
                sys.exit()
            for root, dirnames, inpfilenames in os.walk(path):
                message('Processing directory: %s' % (root))
                for filename in inpfilenames:
                    datafile=os.path.join(root,filename)
                    store_datafile(f_i,datafile,ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,checkfiles=checkfiles,nocert=nocert)

                    if createDDO and not noXML and not load_metadata:
                        createDDO=False
                        message('Due to errors in ingesting metadata file DDO will not be created for files in %s' % (xmlfilename))

                    if createDDO:
                        for sdc_i in SDCList:
                            if sdc_i != SDC:
                                ddo_input=fillDDO(filenames,parentddotype,parentddoid,sdc_i,purpose)
                                store_metadatafile(ddo_input,ec_environment,'DSS',username,password)
        else:
            if noXML:
                datafile=path
                if localpath:
                    datafile=os.path.join(localpath,path)
                    if path in localfiles:
                        datafile=localfiles[datafile]
                store_datafile(path,datafile,ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,checkfiles=checkfiles,nocert=nocert)
                if createDDO:
                    head, filename = os.path.split(path)
                    parentddotype='TEST'
                    parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
                    purpose='TEST'
                    for sdc_i in SDCList:
                        if sdc_i != SDC:
                            ddo_input=fillDDO([filename],parentddotype,parentddoid,sdc_i,purpose)
                            store_metadatafile(ddo_input,ec_environment,'DSS',username,password)
            else:
                if not username or not password:
                    message('Specify username and password')
                    sys.exit()
                filenames=load_xml_file(sys.argv[2])
                if skipfiles:
                    message('Skipping following files: %s' % str(filenames))
                else:
                    for f_i in filenames:
                        localpath_f_i=None
                        if localpath:
                            localpath_f_i=os.path.join(localpath,f_i)
                            if f_i in localfiles:
                                localpath_f_i=localfiles[f_i]
                        if not store_datafile(f_i,localpath_f_i,ec_environment,SDC,username=username,password=password,useoldfiles=useoldfiles,checkfiles=checkfiles,nocert=nocert):
                            sys.exit()
                if not noXML:
                    load_metadata = store_metadatafile(path,ec_environment,userproject, username, password)
                else:
                    message('Metadata files is skipped: %s' % (xmlfilename))
                if createDDO and not noXML and not load_metadata:
                    createDDO=False
                    message('Due to errors in ingesting metadata file DDO will not be created for files in %s' % (xmlfilename))

                if createDDO:
                    parentddotype='TEST'
                    parentddoid=int(100*(datetime.datetime.now()-datetime.datetime(1990,1,1,1,1,1)).total_seconds())
                    purpose='TEST'
                    for sdc_i in SDCList:
                        if sdc_i != SDC:
                            ddo_input=fillDDO(filenames,parentddotype,parentddoid,sdc_i,purpose)
                            store_metadatafile(ddo_input,ec_environment,'DSS',username,password)
    elif sys.argv[1] == 'make_local_asy':
        path=sys.argv[2]
        n_files_in_batch = 5
        n_parallel = 5
        timeout = 0

        for param in sys.argv:
            p_kv=param.split("=")
            if len(p_kv)==2:
                if p_kv[0]=='--environment':
                    ec_environment=p_kv[1]
                if p_kv[0]=='--timeout':
                    timeout=int(p_kv[1])
                if p_kv[0]=='--SDCLIST':
                    tmpSDClist=p_kv[1]
                    try:
                        SDCLIST=tmpSDClist.split(',')
                    except:
                        message('SDC list is not well-formed: %s' % (tmpSDClist))
                        sys.exit()
                if p_kv[0]=='--username':
                    username=p_kv[1]
                if p_kv[0]=='--nfilesinbatch':
                    try:
                        n_files_in_batch=int(p_kv[1])
                    except:
                        message("Can not convert to integer nfilesinbatch %s" % p_kv[1])
                        sys.exit()
                if p_kv[0]=='--nparallel':
                    try:
                        n_parallel=int(p_kv[1])
                    except:
                        message("Can not convert to integer nparallel %s" % p_kv[1])
                        sys.exit()
        if not ec_environment:
            message('Specify environment')
            sys.exit()
        for i_SDC in SDCLIST:
            if i_SDC not in DSSserver[ec_environment]:
                message('No such SDC: %s' % i_SDC)
                sys.exit()
            elif len(DSSserver[ec_environment][i_SDC])==0:
                message('%s DSS server record for environmnet %s is not specified' % (i_SDC, ec_environment))
                sys.exit()
        if os.path.isfile(path):
            input_filelist=open(path).read().replace("\r","").split("\n")
            if len(input_filelist)==0:
                message("No files in input list")
                sys.exit()
            batches = grouper(input_filelist, n_files_in_batch*n_parallel, fillvalue='')

            logfile = {}
            for i_SDC in SDCLIST:
                f_name = "Error_%s_%s" % (i_SDC, time.time())
                logfile[i_SDC] = open(f_name,"w")

            for i_b in batches:
                i_b_s = replacenull(i_b)
                for i_SDC in SDCLIST:
                    makelocalSDCasy(i_b_s, DSSserver[ec_environment][i_SDC], i_SDC, n_files_in_batch, username, password, debug, nocert, logfile[i_SDC], timeout)
            for i_SDC in SDCLIST:
                logfile[i_SDC].close()
        else:
            message("No file %s" % path)
            sys.exit()
    elif sys.argv[1] == 'make_local':
        path=sys.argv[2]
        n_files_in_batch = 1
        n_parallel = 10
        timeout = 0

        for param in sys.argv:
            p_kv=param.split("=")
            if len(p_kv)==2:
                if p_kv[0]=='--environment':
                    ec_environment=p_kv[1]
                if p_kv[0]=='--timeout':
                    timeout=int(p_kv[1])
                if p_kv[0]=='--SDCLIST':
                    tmpSDClist=p_kv[1]
                    try:
                        SDCLIST=tmpSDClist.split(',')
                    except:
                        message('SDC list is not well-formed: %s' % (tmpSDClist))
                        sys.exit()
                if p_kv[0]=='--username':
                    username=p_kv[1]
                if p_kv[0]=='--nparallel':
                    try:
                        n_parallel=int(p_kv[1])
                    except:
                        message("Can not convert to integer nparallel %s" % p_kv[1])
                        sys.exit()
        if not ec_environment:
            message('Specify environment')
            sys.exit()
        for i_SDC in SDCLIST:
            if i_SDC not in DSSserver[ec_environment]:
                message('No such SDC: %s' % i_SDC)
                sys.exit()
            elif len(DSSserver[ec_environment][i_SDC])==0:
                message('%s DSS server record for environmnet %s is not specified' % (i_SDC, ec_environment))
                sys.exit()
        if os.path.isfile(path):
            input_filelist=open(path).read().replace("\r","").split("\n")
            if len(input_filelist)==0:
                message("No files in input list")
                sys.exit()
            batches = grouper(input_filelist, n_files_in_batch*n_parallel, fillvalue='')

            logfile = {}
            for i_SDC in SDCLIST:
                f_name = "Error_%s_%s" % (i_SDC, time.time())
                logfile[i_SDC] = open(f_name,"w")

            for i_b in batches:
                i_b_s = replacenull(i_b)
                for i_SDC in SDCLIST:
                    makelocalSDC(i_b_s, DSSserver[ec_environment][i_SDC], i_SDC, username, password, debug, nocert, logfile[i_SDC], timeout)
            for i_SDC in SDCLIST:
                logfile[i_SDC].close()
        else:
            message("No file %s" % path)
            sys.exit()
    else:
        usage()




if __name__ == '__main__':
    main()
