#! python3
# -*- coding: utf-8 -*-

import sqlite3 as lite
import sys
from datetime import date, datetime, timedelta
import statistics as stats

gctimeformat = '%Y%m%d%H%M%S'
utf8stdout = open(1, 'w', encoding='utf-8', closefd=False) # fd 1 is stdout

def out ( str ): 
  print ( str, file=utf8stdout, end='\t' )
  
def outln ():
  print ( '', file=utf8stdout )


def getConvrateForDate ( curfrom, curto, date ):
  if curfrom == curto:
    return 1
  cur = con.cursor()
  cur.execute ( '''
select p.value_num, p.value_denom
	from prices p
	join commodities co on p.commodity_guid=co.guid
	join commodities cu on p.currency_guid=cu.guid
	where type='last' and co.mnemonic=:curfrom and cu.mnemonic=:curto
	order by abs(p.date-:date) limit 1''',
    {'curfrom': curfrom, 'curto': curto, 'date': date.strftime(gctimeformat) } )
  res = cur.fetchone()
  return res[0]/res[1]
  
def getConvrateForPeriod ( curfrom, curto, perstart, perend ):
  if curfrom == curto:
    return 1
  cur = con.cursor()
  cur.execute ( '''
select p.value_num, p.value_denom
	from prices p
	join commodities co on p.commodity_guid=co.guid
	join commodities cu on p.currency_guid=cu.guid
	where type='last' and date between :perstart and :perend
		and co.mnemonic=:curfrom and cu.mnemonic=:curto
union all
select p.value_denom, p.value_num
	from prices p
	join commodities co on p.commodity_guid=co.guid
	join commodities cu on p.currency_guid=cu.guid
	where type='last' and date between :perstart and :perend
		and co.mnemonic=:curto and cu.mnemonic=:curfrom''', 
    {'curfrom': curfrom, 'curto': curto, 'perstart': perstart.strftime(gctimeformat), 'perend': perend.strftime(gctimeformat) } )
  res = cur.fetchall()
  #out ( "%s %s %s" % (repr(perstart), repr(perend), repr(res)) )
  if len(res)==0:
    return ( getConvrateForDate(curfrom,curto,perstart) + getConvrateForDate(curfrom,curto,perend) ) / 2
  else:
    return stats.median ( [x[0]/x[1] for x in res] )

def history ( acc, start, budgetSince, end, intervaler, currency ):
    
  cur = con.cursor()
  intv = intervaler.findstart(start)
  bsum = 0
  bcount = 0
  veryend = intervaler.findend(end)
  while intv < veryend:
    intprev = intv
    intv = intervaler.increment(intv)
    cur.execute ( '''
select sum(cast(quantity_num as numeric(10,2))/100.0) from transactions t join splits s on s.tx_guid=t.guid 
  where s.account_guid=? and t.post_date>=? and t.post_date<?
    and reconcile_state in ('y','c','n')''', (acc['guid'],intprev.strftime(gctimeformat),intv.strftime(gctimeformat)) )
    x = cur.fetchone()
    if x[0] == None:
      balance = 0
    else:
      balance = x[0]
      
    balance *= getConvrateForPeriod ( acc['currency'], currency, intprev, intv )
    
    if intprev < intervaler.findstart(budgetSince):
      #out ( str(balance) )
      acc['history'].append(balance)
      bsum += balance
      bcount += 1
    else:
      acc['future'][intprev] = balance
      bbalance = bsum / bcount
      out ( str(bbalance) )
      out ( str(bbalance-balance) )

      
def plan ( accs, start, budgetSince, end, intervaler ):

  # print table header
  out ( 'Account' )
  out ( 'Currency' )
  intv = intervaler.findstart(budgetSince)
  veryend = intervaler.findend(end)
  while intv < veryend:
    out ( intv.isoformat() )
    out ( intv.isoformat() )
    intv = intervaler.increment(intv)
  outln ()
  out ('')
  out ('')
  intv = intervaler.findstart(start)
  while intv < veryend:
    if intv < budgetSince:
      #out ( 'fact' )
      pass
    else:
      out ( 'budget' )
      out ( 'left' )
    intv = intervaler.increment(intv)
  outln ()

  # get interval history for every expense account
  for acc in accs:
    acc['history'] = []
    acc['future'] = {}
    out ( ':'.join(acc['name']) )
    out ( acc['currency'] )
    history ( acc, start, budgetSince, end, intervaler, acc['currency'] )
    #out ( repr(acc['future']) )
    outln ()
    
    #if acc['currency'] != 'CAD':
    #  out ( ':'.join(acc['name']) )
    #  out ( 'CAD' )
    #  #history ( acc, start, budgetSince, end, intervaler, 'CAD' )
    #  outln ()
    
  # budgets ahead
  out ( 'Total' )
  out ( 'CAD' )
  intv = intervaler.findstart(budgetSince)
  veryend = intervaler.findend(end)
  while intv < veryend:
    intprev = intv
    intv = intervaler.increment(intv)
    totalb = 0
    totals = 0
    for acc in accs:
      convrate = getConvrateForPeriod ( acc['currency'], 'CAD', intprev, intv )
      totalb += stats.mean(acc['history']) * convrate
      totals += acc['future'][intprev] * convrate
    out ( repr(totalb) )
    out ( repr(totals) )
  outln ()

class ivlWeekly:
  def findstart(self,dt):
    return dt - timedelta(dt.weekday())
  def findend(self,dt):
    return dt - timedelta(dt.weekday()) + timedelta(7)
  def increment(self,dt):
    return dt + timedelta(7)

class ivlSemimonthly:
  def findstart(self,dt):
    if dt.day < 16:
      return dt - timedelta(dt.day-1)
    else:
      return dt - timedelta(dt.day-16)
  def findend(self,dt):
    return self.increment ( self.findstart(dt) )
  def increment(self,dt):
    if dt.day < 16:
      return date ( dt.year, dt.month, dt.day+15 )
    if dt.month == 12:
      return date ( dt.year+1, 1, dt.day-15 )
    else:
      return date ( dt.year, dt.month+1, dt.day-15 )

class ivlMonthly:
  def findstart(self,dt):
    return dt - timedelta(dt.day-1)
  def findend(self,dt):
    return self.increment ( self.findstart(dt) )
  def increment(self,dt):
    if dt.month == 12:
      return date ( dt.year+1, 1, dt.day )
    else:
      return date ( dt.year, dt.month+1, dt.day )
  
#print ( sys.getdefaultencoding() )
#print ( sys.getfilesystemencoding() )
#print ( "Encoding is", sys.stdout.encoding )
#out ( sys.argv[1] )#.decode(sys.getfilesystemencoding()) )

con = lite.connect ( sys.argv[1] ) #.decode(sys.getfilesystemencoding()) )

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
  cur.execute ( '''
select name, parent_guid, a.guid, c.mnemonic 
  from accounts a 
    join commodities c on a.commodity_guid=c.guid 
  where (account_type='EXPENSE' or account_type='INCOME') and placeholder=0''' );
  res = cur.fetchall()
  accs = []
  for x in res:
    #out ( [type(x[0]), type(x[1]), type(x[3])] )
    name = []
    parent = x[1]
    acc = { 'guid': x[2], 'currency': x[3], 'history': [], 'future': {} }
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
  
  out ( "WEEKLY BUDGET" )
  outln ()
  iv = ivlWeekly()
  plan ( accs, start, budgetSince, end, iv )

  out ( "SEMIMONTHLY BUDGET" )
  outln ()
  iv = ivlSemimonthly()
  plan ( accs, start, budgetSince, end, iv )
  
  out ( "MONTHLY BUDGET" )
  outln ()
  iv = ivlMonthly()
  plan ( accs, start, budgetSince, end, iv )
  