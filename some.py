#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import sys
#import codecs
#codecs.register(lambda name: codecs.lookup('utf-8') if name == 'cp65001' else None)

def out(unicodeobj): 
  print unicodeobj.encode('utf-8')

#print sys.getdefaultencoding()
#print sys.getfilesystemencoding()
#print "Encoding is", sys.stdout.encoding
out ( sys.argv[1].decode(sys.getfilesystemencoding()) )#.encode('utf-8')#.decode(sys.getfilesystemencoding())

con = lite.connect ( sys.argv[1].decode(sys.getfilesystemencoding()) )

with con:
  
  cur = con.cursor()
  
  # all expense accounts
  cur.execute ( "select name from accounts where account_type='EXPENSE'" );
  res = cur.fetchall()
  #print ( [x[0].encode('utf-8') for x in res] )
  for x in res:
    out ( x[0] ) #.encode('utf-8') )
  