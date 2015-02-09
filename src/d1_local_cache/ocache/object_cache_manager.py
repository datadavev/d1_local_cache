'''
Created on May 30, 2013

@author: vieglais


TODO:
Command line: 
d1cache [options] [operation]

operation:
  update
  purge
  summary
  serve

'''

import os
import logging
import datetime
import time
import yaml
import shutil
import threading
import Queue
from collections import deque
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import SingletonThreadPool
import d1_common.const
import d1_common.types
import d1_common.types.generated.dataoneTypes_1_1 as dataoneTypes
from d1_common import date_time
from d1_client import d1baseclient
from d1_client import cnclient
from d1_client import objectlistiterator
from d1_local_cache.util import mjd
from d1_local_cache.ocache import models

DEFAULT_CACHE_PATH = "dataone_content"
DEFAULT_CACHE_DATABASE = "cache.sqdb"
MAX_WORKER_THREADS = 6

class ObjectCache():
  '''
  '''
  
  def __init__(self, 
               cachePath=DEFAULT_CACHE_PATH,
               dbname=DEFAULT_CACHE_DATABASE,
               baseUrl=None,
               loadData=False,
               instrument=None,
               certificate=None):
    self._log = logging.getLogger("ObjectCache")
    self.instrument = instrument
    self.cachePath = cachePath
    self._dbname = dbname
    self._loadData = loadData
    self.sessionmaker = None
    self.engine = None
    self.config = {}
    self._pidlist = []
    self._maxthreads = MAX_WORKER_THREADS
    self._certificate = certificate
    self.setUp()
    if not baseUrl is None:
      self.config["baseUrl"] = baseUrl


  def _cacheDataBaseName(self):
    fullpath = os.path.abspath(os.path.join(self.cachePath, self._dbname))
    return "sqlite:///%s" % fullpath


  def setUp(self):
    #Create folder structure and database
    fullpath = os.path.abspath(self.cachePath)
    if not os.path.exists(fullpath):
      os.makedirs(fullpath)
    contentpath = os.path.join(fullpath, "content")
    if not os.path.exists(contentpath):
      os.makedirs(contentpath)
    #populate with object formats if necessary
    self.engine = create_engine(self._cacheDataBaseName(),
                                poolclass=SingletonThreadPool, 
                                pool_size=self._maxthreads)
    self.sessionmaker = scoped_session(sessionmaker(bind=self.engine))
    models.Base.metadata.bind = self.engine
    models.Base.metadata.create_all() 
    self.loadState()


  def loadState(self):
    conf = models.PersistedDictionary(self.sessionmaker())
    for k in conf.keys():
      self.config[k] = conf[k]
    

  def storeState(self):
    conf = models.PersistedDictionary(self.sessionmaker())
    for k in self.config.keys():
      conf[k] = self.config[k]


  @property
  def newestEntry(self):
    session = self.sessionmaker()
    tstamp = session.query(func.max(models.CacheEntry.tstamp)).one()[0]
    session.close()
    return tstamp


  @property
  def lastModified(self):
    session = self.sessionmaker()
    tstamp = session.query(func.max(models.CacheEntry.modified)).one()[0]
    session.close()
    return tstamp
    
    
  def populatePidList(self):
    '''Loads all pids from the cache database into a list
    '''
    session = self.sessionmaker()
    self._pidlist = []
    for pid, in session.query(models.CacheEntry.pid):
      self._pidlist.append(pid)
    

  @property
  def pidcount(self):
    session = self.sessionmaker()
    npids = session.query(models.CacheEntry).count()
    session.close()
    return npids


  def purgeContent(self):
    '''Remove all entries from format list, content list, and 
    '''
    self._log.warn("Purging all content from cache.")
    session = self.sessionmaker()
    session.query(models.CacheEntry).delete()
    session.commit()
    session.query(models.ShortUid).delete()
    session.commit()
    session.query(models.D1ObjectFormat).delete()
    session.commit()
    

  @property
  def baseUrl(self):
    res = None
    try:
      res = self.config['baseUrl']
    except KeyError:
      res = d1_common.const.URL_DATAONE_ROOT
      self.config['baseUrl'] = res
    return res


  @baseUrl.setter
  def baseUrl(self, v):
    self.config['baseUrl'] = v


  @property
  def lastLoaded(self):
    try:
      return self.config['lastLoaded']
    except KeyError:
      return 0.0


  @lastLoaded.setter
  def lastLoaded(self, v):
    if isinstance(v, datetime.datetime):
      v = mjd.dateTime2MJD(v)
    self.config['lastLoaded'] = v
    

  def getObjectPath(self, suid, isSystemMetadata=True):
    subf = suid[0:1]
    fname = "%s_content.xml" % suid
    if isSystemMetadata:
      fname = "%s_sysm.xml" % suid
    path = os.path.join(self.cachePath, "content", subf)
    if not os.path.exists(os.path.abspath(path)):
      os.makedirs(os.path.abspath(path))
    return os.path.join(path, fname)


  def getSystemMetadata(self, suid):
    fpath = self.getObjectPath(suid, isSystemMetadata=True)
    xml = file(fpath, 'rb').read()
    xml = xml.replace(u"<accessPolicy/>", u"")
    xml = xml.replace(u"<preferredMemberNode/>", u"")
    xml = xml.replace(u"<blockedMemberNode/>", u"")
    xml = xml.replace(u"<preferredMemberNode></preferredMemberNode>", u"")
    xml = xml.replace(u"<blockedMemberNode></blockedMemberNode>", u"")
    return dataoneTypes.CreateFromDocument(xml)


  def populateObjectFormats(self):
    session = self.sessionmaker()
    client = cnclient.CoordinatingNodeClient(base_url=self.baseUrl)
    models.loadObjectFormats(session, client)
    session.close()


  def loadObjectList(self, objectList):
    self._log.info("Prefetching identifiers...")
    self.populatePidList()
    session = self.sessionmaker()
    n = 0
    self._log.info("Paging through object list...")
    for o in objectList:
      pid = o.identifier.value()
      #if not models.PIDexists(session, pid):
      if not pid in self._pidlist:
        self._log.debug("Adding PID: %s" % pid)
        tmod = mjd.dateTime2MJD( o.dateSysMetadataModified )
        res = models.addObjectCacheEntry(session, 
                                         pid, 
                                         o.formatId,
                                         o.size,
                                         tmod)
        if res is None:
          self._log.error("Could not add PID: %s" % pid)
        else:
          self._log.info("Added PID: %s" % pid)
          n += 1
          if self.instrument is not None:
            self.instrument.gauge("PIDs", n)
    session.close()
    return n
    

  def countByType(self, otype="METADATA", status=None, cstatus=None):
    session = self.sessionmaker()
    try:
      if not status is None:
        res = session.query(models.CacheEntry).join(models.D1ObjectFormat)\
                    .filter( models.D1ObjectFormat.formatType==otype )\
                    .filter(models.CacheEntry.sysmstatus==status)
      elif cstatus is not None:
        res = session.query(models.CacheEntry).join(models.D1ObjectFormat)\
                    .filter( models.D1ObjectFormat.formatType==otype )\
                    .filter(models.CacheEntry.contentstatus==cstatus)        
      else:
        res = session.query(models.CacheEntry).join(models.D1ObjectFormat)\
                    .filter( models.D1ObjectFormat.formatType==otype )
      self._log.debug(str(res))
      return res.count()
    except Exception as e:
      self._log.error(e)
    finally:
      session.close()
    

  def countByTypeDateUploaded(self, otype='METADATA', mjd_uploaded=None):
    '''Returns a count of objects matching the provided object type and 
    optionally older than or equal to date_uploaded.
    
    If specified, mjd_uploaded should be a floating point MJD value.
    '''
    session = self.sessionmaker()
    try:
      if mjd_uploaded is None:
        res = session.query(models.CacheEntry).join(models.D1ObjectFormat) \
                    .filter( models.D1ObjectFormat.formatType == otype )
      else:
        res = session.query(models.CacheEntry).join(models.D1ObjectFormat) \
                    .filter( models.D1ObjectFormat.formatType == otype ) \
                    .filter( models.CacheEntry.uploaded <= mjd_uploaded)
      return res.count()
    except Exception as e:
      self._log.error(e)
    finally:
      session.close()


  def __str__(self):
    '''Return a string representation of self
    '''
    res = {}
    res["baseURL"] = self.baseUrl
    res["count"] = self.pidcount
    res["lasttimestamp"] = self.newestEntry
    res["newestobject"] = self.lastModified
    res['numzerostatus'] = 0
    counts = {}
    for otype in ["DATA", "METADATA", "RESOURCE"]:
      counts[otype] = self.countByType(otype=otype, status=None)
    res['counts'] = counts
    counts = {}
    for otype in ["DATA", "METADATA", "RESOURCE"]:
      counts[otype] = self.countByType(otype=otype, status=0)
    res['zcounts'] = counts
    counts = {}
    for otype in ["DATA", "METADATA", "RESOURCE"]:
      counts[otype] = self.countByType(otype=otype, status=200)
    res['okcounts'] = counts
    counts = {}
    for otype in ["DATA", "METADATA", "RESOURCE"]:
      counts[otype] = self.countByType(otype=otype, cstatus=200)
    res['cokcounts'] = counts
    return yaml.dump(res)
    

  def loadSystemMetadata(self):
    #Queue to hold the tasks that need to be processed
    Q = Queue.Queue()
    CQ = deque([],100)
    
    def worker():
      '''Pulls PIDs off the queue and downloads the associated system metadata,
      updates the cache database.
      '''
      _log = logging.getLogger("loadSysmeta.worker.%s" % str(threading.current_thread().ident))
      client = d1baseclient.DataONEBaseClient(self.baseUrl, 
                                              cert_path=self._certificate)
      tsession = self.sessionmaker()
      moreWork = True
      while moreWork:
        idx, pid = Q.get()
        _log.info( "Loading system metadata for %s" % pid )
        try:
          wo = tsession.query(models.CacheEntry).get(pid)
          sysmeta = client.getSystemMetadataResponse(pid)
          #sysmeta.read()
          #sysmeta.close()
          spath = self.getObjectPath(wo.suid.uid, isSystemMetadata=True)
          wo.sysmeta = spath
          fdest = open(os.path.abspath(spath), "wb")
          shutil.copyfileobj(sysmeta, fdest)
          fdest.close()
          wo.sysmstatus = sysmeta.status
          tsession.commit()
          CQ.append(time.time())
        except d1_common.types.exceptions.DataONEException as e:
          _log.error(e)
        except Exception as e:
          _log.warn("Unanticipated exception for pid: %s" % pid)
          _log.error(e)
        finally:
          pass
        Q.task_done()
        moreWork = not Q.empty()
        if self.instrument is not None:
          self.instrument.gauge("QSize", Q.qsize())
          if (len(CQ) == CQ.maxlen):
            try:
              dt = CQ.maxlen / ((CQ[CQ.maxlen-1] - CQ[0]))
              self.instrument.gauge('sysm.sec-1', "{:.3f}".format(dt))
            except Exception as e:
              _log.error(e)
      tsession.close()
      _log.debug("Thread %s terminated." % str(threading.current_thread().ident))

    #20 seems about right
    nworkers = self._maxthreads - 1 
    #stage the workers
    for i in range(nworkers):
      wt = threading.Thread(target = worker)
      wt.daemon = True
      wt.start()
      self._log.debug("Thread %d as %s started" % (i, str(wt.ident)))
    
    #Get the list of PIDs to work with
    session = self.sessionmaker()
    #work = session.query(models.CacheEntry).join(models.D1ObjectFormat)\
    #              .filter( or_( models.D1ObjectFormat.formatType=="METADATA", 
    #                            models.D1ObjectFormat.formatType=="RESOURCE"))\
    #              .filter(models.CacheEntry.sysmstatus==0)
    #Load system metadata for everything
    work = session.query(models.CacheEntry)\
                  .filter(models.CacheEntry.sysmstatus==0)
    i=0
    for o in work:
      Q.put( [i, o.pid] )
      i += 1
    session.close()
    Q.join()
  
  
  def loadContent(self, nthreads=1):
    work_queue = Queue.Queue()
    
    #++++++++++++++++++++++++++++++++++
    def worker():
      _log = logging.getLogger("loadContent.worker.%s" % str(threading.current_thread().ident))
      client = d1baseclient.DataONEBaseClient(self.baseUrl,
                                              cert_path=self._certificate)
      tsession = self.sessionmaker()
      moreWork = True
      while moreWork:
        idx, pid = work_queue.get()
        _log.info( "Loading content for %s" % pid )
        try:
          wo = tsession.query(models.CacheEntry).get(pid)
          content = client.getResponse(pid)
          cpath = self.getObjectPath(wo.suid.uid, isSystemMetadata=False)
          wo.content = cpath
          fdest = open(os.path.abspath(cpath), "wb")
          shutil.copyfileobj(content, fdest)
          fdest.close()
          wo.contentstatus = content.status
          tsession.commit()
        except d1_common.types.exceptions.DataONEException as e:
          _log.error(e)
        except Exception as e:
          _log.warn("Unanticipated exception for pid: %s" % pid)
          _log.error(e)
        finally:
          pass
        work_queue.task_done()
        moreWork = not work_queue.empty()
      tsession.close()
    #----------------------------------
    
    nworkers = self._maxthreads - 1
    #stage the workers
    for i in range(nworkers):
      wt = threading.Thread(target = worker)
      wt.daemon = True
      wt.start()
      self._log.debug("Thread %d as %s started" % (i, str(wt.ident)))
      
    session = self.sessionmaker()
    work = session.query(models.CacheEntry).join(models.D1ObjectFormat)\
                  .filter( or_( models.D1ObjectFormat.formatType=="METADATA", 
                                models.D1ObjectFormat.formatType=="RESOURCE"))\
                  .filter(models.CacheEntry.contentstatus==0)
    #work = session.query(models.CacheEntry).join(models.D1ObjectFormat)\
    #              .filter( models.D1ObjectFormat.formatType=="RESOURCE")\
    #              .filter(models.CacheEntry.contentstatus==0)
    i = 0
    for o in work:
      work_queue.put( [i, o.pid] )
      i += 1
    session.close()
    work_queue.join()

    
  def loadSysmetaContent(self, startTime=None, startFrom=None,
                         onNextPage=None):
    maxtoload = -1
    pagesize = 1000
    start = startFrom
    if startFrom is None:
      start = self.pidcount-1
    if isinstance(startTime, float):
      #Assume the provided startTime is a MJD
      startTime = date_time.to_xsd_datetime( mjd.MJD2dateTime( startTime ))
    self._log.info("Starting PID load from: %d" % start)
    if start < 0:
      start = 0;
    self.lastLoaded = mjd.now()
    client = d1baseclient.DataONEBaseClient(self.baseUrl, 
                                            cert_path=self._certificate)
    self._log.info( "Loading identifiers..." )
#    objects = objectlistiterator.ObjectListIterator(client, start=start,
#                            pagesize=pagesize,
#                            max=maxtoload,
#                            fromDate=startTime,
#                            pagecallback=onNextPage)
    objects = objectlistiterator.ObjectListIterator(client, start=start,
                            pagesize=pagesize,
                            max=maxtoload,
                            fromDate=startTime)
    n = self.loadObjectList(objects)
    self._log.info( "Added %d identifiers" % n )
    self._log.info( "Loading System Metadata..." )
    self.loadSystemMetadata()
    #self._log.info( "Loading content..." )
    #self.loadContent()
    self.storeState()
    self._log.info( "Done." )
    
    
  
  def adjustSysMetaentries(self):
    '''Iterate through all system metadata entries and:
    
    1. adjust the path to be relative 
    2. populate a new column for the dateUploaded
    3. populate a new column for the originMemberNode
    4. populate a new column for archived
    5. populate a new column for obsoletedBy
    '''
    session = self.sessionmaker()
    work = session.query(models.CacheEntry)\
                  .filter(models.CacheEntry.uploaded==0)
    counter = 0
    total = 0                  
    for o in work:
      self._log.info("{0:s} : {1:s}".format(o.suid.uid, o.pid))
      sysm = self.getSystemMetadata(o.suid.uid)
      
      o.uploaded = mjd.dateTime2MJD(sysm.dateUploaded)
      o.archived = sysm.archived
      o.origin = sysm.originMemberNode.value()
      if sysm.obsoletes is not None:
        o.obsoletes = sysm.obsoletes.value()
      if sysm.obsoletedBy is not None:
        o.obsoleted_by = sysm.obsoletedBy.value()
      counter += 1
      total += 1
      if counter > 1000:
        session.commit()
        counter = 0
      self.instrument.gauge('sysm.fix', total)
    session.commit()
      

