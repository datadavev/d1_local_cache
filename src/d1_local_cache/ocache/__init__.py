'''Implements an on disk cache for DataONE objects.

The cache is implemented as a hierarchy of folders to contain copies of
system metadata, science metadata, resoruce maps, and optionally data objects.

Hierarchy:

- cache
  +- cache.sqdb
  +- content
     +- A
        +- Axxxx_sysm
  +- 

cache.sqdb is a python Shove persistent dictionary with entries:

  key = PID
  value = [timestamp,
           status,
           local_path, 
           ]

PID, localname, status, timestamp

'''



