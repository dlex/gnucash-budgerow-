#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3 as lite
import sys
from datetime import date, datetime, timedelta
#import codecs
#codecs.register(lambda name: codecs.lookup('utf-8') if name == 'cp65001' else None)

gctimeformat = '%Y%m%d%H%M%S'

def out ( unicodeobj ): 
  sys.stdout.write ( unicodeobj.encode('utf-8') )
  sys.stdout.write ( '\t' )

def plan ( con, accs, start, budgetSince, end, intervaler ):

  # print table header
  out ( 'Account' )
  out ( 'Currency' )
  weekst = intervaler.findstart(start)
  veryend = intervaler.findend(end)
  while weekst < veryend:
    out ( weekst.isoformat() )
    weekst = intervaler.increment(weekst)
  print
  out ('')
  out ('')
  weekst = intervaler.findstart(start)
  while weekst < veryend:
    if weekst < budgetSince:
      out ( 'fact' )
    else:
      out ( 'budget' )
    weekst += timedelta(7)
  print

  # get intervally history for every expense account
  for acc in accs:
    weekst = intervaler.findstart(start)
    out ( ':'.join(acc['name']) )
    out ( acc['currency'] )
    bsum = 0
    bcount = 0
    while weekst < veryend:
      weeklo = weekst
      weekst = intervaler.increment(weekst)
      cur.execute ( "select sum(cast(quantity_num as numeric(10,2))/100.0) from transactions t join splits s on s.tx_guid=t.guid "
        "where s.account_guid=? and t.post_date>=? and t.post_date<?"
        "and reconcile_state in ('y','c','n')", (acc['guid'],weeklo.strftime(gctimeformat),weekst.strftime(gctimeformat)) )
      x = cur.fetchone()
      if x[0] == None:
        balance = 0
      else:
        balance = x[0]
        
      if weeklo < budgetSince:
        out ( str(balance) )
        bsum += balance
        bcount += 1
      else:
        bbalance = bsum / bcount + balance
        out ( str(bbalance) )
    print

class ivlWeekly:
  def findstart(self,dt):
    return dt - timedelta(dt.weekday())
  def findend(self,dt):
    return dt - timedelta(dt.weekday()) + timedelta(7)
  def increment(self,dt):
    return dt + timedelta(7)
#print sys.getdefaultencoding()
#print sys.getfilesystemencoding()
#print "Encoding is", sys.stdout.encoding
#out ( sys.argv[1].decode(sys.getfilesystemencoding()) )

con = lite.connect ( sys.argv[1].decode(sys.getfilesystemencoding()) )

with con:
  
  cur = con.cursor()
  
  # time range
  cur.execute ( "select min(post_date), max(post_date) from transactions" );
  x = cur.fetchone();
  #dt = datetime.strptime ( x[0], gctimeformat );
  start = datetime.strptime ( '20150101000000', gctimeformat ).date()
  #dt = datetime.strptime ( x[1], gctimeformat );
  dt = date.today()
  budgetSince = dt - timedelta(dt.weekday())
  dt = date.today()
  end = dt + timedelta(30)
  #print ( verystart, veryend, budgetSince );
  
  # all expense accounts
  cur.execute ( "select name, parent_guid, a.guid, c.mnemonic from accounts a join commodities c on a.commodity_guid=c.guid where account_type='EXPENSE' and placeholder=0" );
  res = cur.fetchall()
  accs = []
  for x in res:
    name = []
    parent = x[1]
    acc = { 'guid': x[2], 'currency': x[3] }
    cparentacc = con.cursor()
    while parent != None:
      name.append ( x[0] )
      cparentacc.execute ( "select name, parent_guid from accounts where guid=?", (parent,) )
      x = cparentacc.fetchone()
      parent = x[1]
    acc['name'] = [i for i in reversed(name)]
    #out ( ':'.join(acc['name']) )
    accs.append(acc)
    
  # now accs contains list of expense accounts we are interested in 
  
  iv = ivlWeekly()
  plan ( con, accs, start, budgetSince, end, iv )
