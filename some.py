#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import sys
from datetime import datetime, timedelta
#import codecs
#codecs.register(lambda name: codecs.lookup('utf-8') if name == 'cp65001' else None)

def out(unicodeobj): 
  print unicodeobj.encode('utf-8')

gctimeformat = '%Y%m%d%H%M%S'
  
#print sys.getdefaultencoding()
#print sys.getfilesystemencoding()
#print "Encoding is", sys.stdout.encoding
out ( sys.argv[1].decode(sys.getfilesystemencoding()) )#.encode('utf-8')#.decode(sys.getfilesystemencoding())

con = lite.connect ( sys.argv[1].decode(sys.getfilesystemencoding()) )

with con:
  
  cur = con.cursor()
  
  # time range
  cur.execute ( "select min(post_date), max(post_date) from transactions" );
  x = cur.fetchone();
  #dt = datetime.strptime ( x[0], gctimeformat );
  dt = datetime.strptime ( '20150101000000', gctimeformat );
  verystart = dt - timedelta(dt.weekday())
  dt = datetime.strptime ( x[1], gctimeformat );
  veryend = dt - timedelta(dt.weekday()) + timedelta(7)
  print ( verystart, veryend );
  
  # all expense accounts
  cur.execute ( "select name, parent_guid, guid from accounts where account_type='EXPENSE' and placeholder=0" );
  res = cur.fetchall()
  #print ( [x[0].encode('utf-8') for x in res] )
  accs = []
  for x in res:
    name = []
    parent = x[1]
    accguid = x[2]
    cparentacc = con.cursor()
    while parent != None:
      name.append ( x[0] )
      cparentacc.execute ( "select name, parent_guid from accounts where guid=?", (parent,) )
      x = cparentacc.fetchone()
      parent = x[1]
    acc = { 'name': [i for i in reversed(name)], 'guid': accguid }
    out ( ':'.join(acc['name']) ) #.encode('utf-8') )
    accs.append(acc)
    
  # now accs contains list of expense accounts we are interested in 
  
  # get weekly history for every expense account
  for acc in accs:
    weekst = verystart
    balance = 0
    out ( ':'.join(acc['name']) )
    print ( x )
    while weekst < veryend:
      weekst += timedelta(7)
      cur.execute ( "select sum(cast(value_num as numeric(10,2))/100.0) from transactions t join splits s on s.tx_guid=t.guid "
        "where s.account_guid=? and t.post_date<? "
        "and reconcile_state in ('y','c','n')", (acc['guid'],weekst.strftime(gctimeformat)) )
      x = cur.fetchone()
      if x[0] == None:
        newbalance = 0
      else:
        newbalance = x[0]
      print weekst, newbalance-balance
      balance = newbalance
      