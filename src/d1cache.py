'''
'''
import sys
import os
import logging
import yaml
import datetime
from d1_local_cache.util import mjd

HOMEPATH=".dataone"
CONFIGFILE="cache.conf"

def readConfiguration():
  fbase = os.path.join(os.environ["HOME"], HOMEPATH)
  fconfig = os.path.join(fbase, CONFIGFILE)
  conf = {'sysmcache': {'database':'cache.sqdb',
                        'path':fbase,
                        'cert': None,
                        'instrument':'d1cache'},
          'environment':{'name':'production',
                        'baseurl':'https://cn.dataone.org/cn'},
          'python':{'path':[]},
          }
  yconf = {}
  try:
    yconf = yaml.load(file(fconfig))
  except Exception as e:
    logging.warn(e)
  for k in yconf:
    conf[k] = yconf[k]
  return conf


def setupEnvironment(paths):

  '''Set python path and load modules
  
  paths : list of paths to add to sys.path
  '''
  import sys
  sys.path = sys.path + paths


def onNextPage(total, start, pagesize, begin):
  if begin:
    logging.info("Object list iterator start=%d/%d (%d) begin" \
                 % (start, total, pagesize))
  else:
    logging.info("Object list iterator start=%d/%d (%d) end" \
                 % (start, total, pagesize))


def countObjectTypes(cache):
  def mkdate(sdate):
    dt = datetime.datetime.strptime(sdate, "%Y-%m-%d")
    return dt
  
  session = cache.sessionmaker()
  dates = [mkdate("2012-07-01"),
           mkdate("2012-07-31"),
           mkdate("2012-08-30"),
           mkdate("2012-09-29"),
           mkdate("2012-10-29"),
           mkdate("2012-11-28"),
           mkdate("2012-12-28"),
           mkdate("2013-01-27"),
           mkdate("2013-02-26"),
           mkdate("2013-03-28"),
           mkdate("2013-04-27"),
           mkdate("2013-05-27"),
           mkdate("2013-06-26"),
           mkdate("2013-07-26"),
           mkdate("2013-08-25"),
           mkdate("2013-09-24"),
           mkdate("2013-10-24"),
           mkdate("2013-11-23"),
           mkdate("2013-12-23"),
           mkdate("2014-01-22"),
           mkdate("2014-02-21"),
           mkdate("2014-03-23"),
           ]
  print ("Date,Data,Metadata,ResourceMap")
  for cdate in dates:
    res = [cdate.strftime("%Y-%m-%d"), "0","0","0"]
    adate = mjd.dateTime2MJD(cdate) 
    res[1] = str(cache.countByTypeDateUploaded(otype="DATA", mjd_uploaded=adate))
    res[2] = str(cache.countByTypeDateUploaded(otype="METADATA", mjd_uploaded=adate))
    res[3] = str(cache.countByTypeDateUploaded(otype="RESOURCE", mjd_uploaded=adate))
    print (",".join(res))


OP_STATE="state"
OP_UPDATE="update"
OP_COUNT="count"

def main(operation=OP_STATE):
  conf = readConfiguration()
  #setupEnvironment(conf['python']['path'])
  from d1_local_cache.ocache.object_cache_manager import ObjectCache
  from d1_local_cache.util import instrument
  
  instrument = instrument.StatsdClient(prefix=conf['sysmcache']['instrument'])
  
  cache = ObjectCache(cachePath=conf['sysmcache']['path'],
                    dbname=conf['sysmcache']['database'],
                    baseUrl = conf['environment']['baseurl'],
                    instrument=instrument,
                    certificate=conf['sysmcache']['cert'])

  if operation == OP_STATE:
    print str(cache)
    return
  
  if operation == OP_UPDATE:
    newest = cache.lastModified
    #newest = "2013-05-20T17:42:54.000000+00:00"
    logging.info("Start date is: %s" % newest)
    #purgeEverything(cache)
    cache.populateObjectFormats()
    cache.loadSysmetaContent(startTime=newest, startFrom=0, onNextPage=onNextPage)
    return

  if operation == OP_COUNT:
    countObjectTypes(cache)
    return
  
  
  

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(OP_STATE)
  main(OP_UPDATE)
  #main(OP_COUNT)
