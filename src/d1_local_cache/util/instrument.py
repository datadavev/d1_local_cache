'''Implements an instrument class that can be used to send gauge measurements
to the statsd listener.

Values may be monitored by watching:

  https://statsd.dataone.org/statsd2ws/[?f=PREFIX*]

where
  ?f=PREFIX* is an optional filter to limit the instruments to those with 
    names that start with PREFIX  
'''
import socket
import logging

STATSD_HOST = "statsd.dataone.org"
STATSD_PORT = 8125


class StatsdClient(object):
  
  def __init__(self, 
               host=STATSD_HOST, 
               port=STATSD_PORT, 
               prefix=""):
    self.addr = (host, port)
    self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if len(prefix) >= 0:
      prefix += "."
    self._prefix = prefix


  def close(self):
    self._udp.close()


  def send(self, message):
    if self.addr[0] == "null":
      return
    try:
      self._udp.sendto(message, self.addr)
    except Exception as e:
      logging.error("Bummer: %s" % str(e))

  def gauge(self, name, value):
    msg = "%s%s:%s|g" % (self._prefix, name, str(value))
    self.send(msg)

  def sendMeasurements(self, measurements):
    sout = []
    for k in measurements.keys():
      sout.append("%s: %s" % (k, str(measurements[k])))
      self.gauge(k, measurements[k])
    print ", ".join(sout)

