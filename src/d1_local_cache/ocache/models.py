'''
Created on May 29, 2013

@author: vieglais

Provides SQLAlchemy ORM models for simple representation of object formats and
system metadata in a relational database. Note that this does not provide a 
full decomposition of the system metadata. The primary goals are to support an
updateable local cache of system metadata retrieved from a CN and to provide
an environment for programmatic access to the system metadata and optionally 
the actual objects identified by PIDs.
'''

import logging
from UserDict import DictMixin
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float, PickleType 
from sqlalchemy import BigInteger
from sqlalchemy.orm import relationship, backref, exc
from d1_local_cache.util import mjd
from d1_local_cache.util import shortUidgen

Base = declarative_base()

#===============================================================================

class CacheMeta(Base):
  '''Implements a key value store. Used for maintaining state of the cache.
  '''
  __tablename__ = "meta"
  
  key = Column(String, primary_key=True)
  value = Column(PickleType)
  
  def __init__(self, k, v):
    self.key = k
    self.value = v
  
  def __repr__(self):
    return "<CacheMeta('%s', '%s')>" % (self.key, repr(self.value))


#===============================================================================

class ShortUid(Base):
  '''Provide a model that maps between a short, file system friendly unique 
  identifier and the DataONE PID for the object.
  '''
  __tablename__ = 'shortuid'

  id = Column(Integer, primary_key=True, autoincrement=True)
  uid = Column(String)

  def __init__(self):
    pass

  def __repr__(self):
    return u"<ShortUid(%d,'%s')>" % (self.id, self.uid)


#===============================================================================

class D1ObjectFormat(Base):
  '''Provides a model for storing basic information about object formats.
  '''
  __tablename__ = 'formatid'  
  formatId = Column(String, primary_key=True)
  formatType = Column(String)
  name = Column(String)
  
  def __init__(self, formatId, formatType, name):
    self.formatId = formatId
    self.formatType = formatType
    self.name = name

  def __repr__(self):
    return u"<D1ObjectFormats('%s', '%s', '%s')>" % (self.formatType,
                                                    self.formatId,
                                                    self.name)

#===============================================================================

class CacheEntry(Base):
  '''The model representing system metadata in the database. Only single value
  properties are stored here. Richer interpretation of the system metadata would
  require processing the cached copies of system metadata documents.
  '''
  __tablename__ = "cacheentry"
  
  pid = Column(String, primary_key=True)
  suid_id = Column(Integer, ForeignKey("shortuid.id"))
  format_id = Column(String, ForeignKey("formatid.formatId"))
  tstamp = Column(Float)
  sysmstatus = Column(Integer, default=0)
  contentstatus = Column(Integer, default=0)
  sysmeta = Column(String, default=None)
  content = Column(String, default=None)
  localpath = Column(String)
  size = Column(BigInteger, default=0)
  modified = Column(Float)
  
  #New fields
  uploaded = Column(Float) #dateUploaded
  archived = Column(Integer) #boolean
  origin = Column(String) #origin member node
  obsoletes = Column(String) 
  obsoleted_by = Column(String)
  
  suid = relationship("ShortUid", uselist=False, backref=backref("shortuid"))
  format = relationship("D1ObjectFormat", uselist=False, 
                        backref=backref("shortuid"))
  
  def __init__(self, suid, pid, format, size, modified):
    self.suid = suid
    self.format = format
    self.pid = pid
    self.tstamp = mjd.now()
    self.size = size
    self.modified = modified
  
  def __repr__(self):
    return u"<CacheEntry('%s', '%s', %d, %.5f, '%s')>" % \
           (self.pid, self.suid.uid, self.sysmstatus, self.tstamp, self.format)
    
#===============================================================================

class PersistedDictionary(DictMixin):
  
  def __init__(self, session):
    #super(PersistedDictionary, self).__init__()
    self.session = session
  

  def __getitem__(self, key):
    res = self.session.query(CacheMeta).get(key)
    if res is None:
      raise KeyError()
    return res.value

  
  def __setitem__(self, key, value):
    try:
      entry = self.session.query(CacheMeta).get(key)
      entry.value = value
    except:
      entry = CacheMeta(key, value)
      self.session.add(entry)
    self.session.commit() 
  

  def __delitem__(self, key):
    item = self.__getitem__(key)
    self.session.delete(item)
  

  def keys(self):
    res = self.session.query(CacheMeta.key).all()
    try:
      return zip(*res)[0]
    except IndexError:
      return []

#===============================================================================

    
def createShortUid(session):
  uid = ShortUid()
  session.add(uid)
  session.flush()
  uid.uid = shortUidgen.encode_id(uid.id)
  session.flush()
  return uid


def createCacheEntry(session, pid, format, size, tmod):
  '''Creates a new cache entry and commits it to the database.
  '''
  suid = createShortUid(session)
  centry = CacheEntry(suid, pid, format, size, tmod)
  session.add(centry)
  session.flush()
  session.commit()
  return centry


def getFormatByFormatId(session, formatId):
  '''Return a format entry given a formatId. The formatId values are identical
  to what is used by the CN from which the cache was generated. 
  '''
  try:
    res = session.query(D1ObjectFormat)\
                 .filter(D1ObjectFormat.formatId == formatId).one()
    return res
  except exc.NoResultFound:
    pass
  return None


def addObjectCacheEntry(session, pid, formatId, size, tmod):
  '''Adds a new minimal entry in the cache entry table.
  '''
  if getEntryByPID(session, pid) is None:
    format = getFormatByFormatId(session, formatId)
    return createCacheEntry(session, pid, format, size, tmod)
  return None


def PIDexists(session, pid):
  '''Return true if the given PID is recorded in the cache.
  '''
  q = session.query(CacheEntry).filter(CacheEntry.pid == pid)
  return session.query(q.exists())


def getEntryByPID(session, pid):
  '''Return a cache entry given the PID or None if the PID isn't recorded.
  '''
  try:
    #res = session.query(CacheEntry).filter(CacheEntry.pid == pid).one()
    res = session.query(CacheEntry).get(pid)
    return res
  except exc.NoResultFound:
    pass
  return None


def getEntryBySUID(session, suid):
  '''Return a cache entry given a short ID (i.e. a local proxy for the PID)
  '''
  res = session.query(CacheEntry).join(ShortUid).\
        filter(ShortUid.uid==suid).one()
  return res


def loadObjectFormats(session, client):
  '''Populates the formatId table with formats retrieved by the provided client.
  '''
  log = logging.getLogger("LoadObjectFormats")
  formats = client.listFormats()
  for format in formats.objectFormat:
    logging.debug(format)
    testfmt = getFormatByFormatId(session, format.formatId)
    if testfmt is None:
      fmt = D1ObjectFormat(format.formatId,
                           format.formatType,
                           format.formatName)
      session.add(fmt)
  session.commit()


#===============================================================================  

if __name__ == "__main__":
  import unittest
  from sqlalchemy import *
  from sqlalchemy.orm import *
  from d1_client import cnclient
  
  class TestCacheEntry(unittest.TestCase):
    
    def setUp(self):
      self.engine = create_engine("sqlite:///:memory:", echo=True)
      self.sessionmaker = scoped_session(sessionmaker(bind=self.engine))
      Base.metadata.bind = self.engine
      Base.metadata.create_all()
      #populate the formatID table
      client = cnclient.CoordinatingNodeClient()
      session = self.sessionmaker()
      loadObjectFormats(session, client)
      session.close()
      

    def test_cacheentry(self):
      session = self.sessionmaker()
      for i in xrange(0,10):
        ce = addObjectCacheEntry(session, "A_%.2d" % i,  
                                 "FGDC-STD-001.1-1999", 1, mjd.now())
        print ce
        print ce.suid
      
      suid = "n242n"
      res = getEntryBySUID(session, suid)
      self.assertEqual(suid, res.suid.uid)
      self.assertEqual("A_07", res.pid)
      print res
      
      pid = "A_03"
      res = getEntryByPID(session, pid)
      self.assertEqual(pid, res.pid)
      print res
      
      session.close()
  
    def test_dict(self):
      session = self.sessionmaker()
      d = PersistedDictionary(session)
      d["test"] = {"a":"b"}
      d["2"] = 1234
      self.assertEqual(d.keys(), ("test", "2" ))
      self.assertEqual(2, len(d))
      self.assertEqual(1234, d["2"])
      d["2"] = 5678
      self.assertEqual(5678, d["2"])
      session.close()
  
  #logging.basicConfig(level=logging.DEBUG)
  unittest.main()
