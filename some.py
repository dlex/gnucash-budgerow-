﻿#! python3
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

    
def history ( acc, start, budgetSince, end, intervaler, currency, printfact ):
    
  cur = con.cursor()
  intv = intervaler.findstart ( start )
  bsum = 0
  bcount = 0
  veryend = intervaler.findend(end)
  while intv < veryend:
    intvNext = intervaler.increment(intv)
    if intv < acc['hstart']:
      if printfact:
        out ( '' )
      intv = intvNext
      continue
    cur.execute ( '''
select sum(cast(quantity_num as numeric(10,2))/100.0) from transactions t join splits s on s.tx_guid=t.guid 
  where s.account_guid=? and t.post_date>=? and t.post_date<?
    and reconcile_state in ('y','c','n')''', (acc['guid'],intv.strftime(gctimeformat),intvNext.strftime(gctimeformat)) )
    x = cur.fetchone()
    if x[0] == None:
      balance = 0
    else:
      balance = x[0]
      
    balance *= getConvrateForPeriod ( acc['currency'], currency, intv, intvNext )
    
    if intv < intervaler.findstart(budgetSince):
      if printfact:
        out ( str(balance) )
      acc['history'].append(balance)
      bsum += balance
      bcount += 1
    else:
      bbalance = bsum / bcount
      if acc['partic'] == 0:
        bbalance = balance
      acc['future'][intv] = balance
      out ( str(bbalance) )
      out ( str(bbalance-balance) )
      
    intv = intvNext

      
def plan ( accs, start, budgetSince, end, intervaler, printfact ):

  # print table header
  out ( 'Account' )
  out ( 'Participation' )
  out ( 'Currency' )
  intv = intervaler.findstart(start)
  veryend = intervaler.findend(end)
  while intv < veryend:
    if intv < budgetSince:
      if printfact:
        out ( intv.isoformat() )
    else:
      out ( intv.isoformat() )
      out ( intv.isoformat() )
    intv = intervaler.increment(intv)
  outln ()
  out ('')
  out ('')
  out ('')
  intv = intervaler.findstart(start)
  while intv < veryend:
    if intv < budgetSince:
      if printfact:
        out ( 'fact' )
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
    out ( acc['partic'] )
    out ( acc['currency'] )
    history ( acc, start, budgetSince, end, intervaler, acc['currency'], printfact )
    #out ( repr(acc['future']) )
    outln ()
    
    #if acc['currency'] != 'CAD':
    #  out ( ':'.join(acc['name']) )
    #  out ( 'CAD' )
    #  #history ( acc, start, budgetSince, end, intervaler, 'CAD' )
    #  outln ()
    
  # budgets ahead
  out ( 'Total' )
  out ( '' )
  out ( 'CAD' )
  intv = intervaler.findstart(start)
  veryend = intervaler.findend(end)
  while intv < veryend:
    intvNext = intervaler.increment(intv)
    totalb = 0
    totals = 0
    if intv < budgetSince:
      if printfact:
        out ( '' )
    else:
      for acc in accs:
        convrate = getConvrateForPeriod ( acc['currency'], 'CAD', intv, intvNext )
        #out ( "%s %s" % (acc['name'], acc['future']) )
        accfuture = acc['future'][intv] * convrate 
        if acc['partic'] == 0:
          totals += accfuture
        else:
          totalb += stats.mean(acc['history']) * convrate
        totals += accfuture
      out ( repr(totalb) )
      out ( repr(totals) )
    intv = intvNext
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
  #out ( [start, end, budgetSince] );
  
  # all I/E accounts
  cur.execute ( '''
select name, parent_guid, a.guid, c.mnemonic, coalesce(ba.participation,1), ba.history_start
  from accounts a 
    join commodities c on a.commodity_guid=c.guid 
    left outer join bw_accounts ba on ba.guid=a.guid
  where (account_type='EXPENSE' or account_type='INCOME') and placeholder=0''' );
  res = cur.fetchall()
  accs = []
  for x in res:
    #out ( [type(x[0]), type(x[1]), type(x[3])] )
    name = []
    parent = x[1]
    acc = { 'guid': x[2], 'currency': x[3], 'partic': x[4], 'hstart': x[5], 'history': [], 'future': {} }
    cparentacc = con.cursor()
    while parent != None:
      name.append ( x[0] )
      cparentacc.execute ( '''
select name, parent_guid, ba.history_start
  from accounts a
    left outer join bw_accounts ba on ba.guid=a.guid
  where a.guid=?''', (parent,) )
      x = cparentacc.fetchone()
      parent = x[1]
      if acc['hstart'] == None:
        acc['hstart'] = x[2]
    acc['name'] = [i for i in reversed(name)]
    if acc['hstart'] != None:
      acc['hstart'] = datetime.strptime ( acc['hstart'], gctimeformat ).date()
    #out ( ':'.join(acc['name']) )
    accs.append(acc)
  accs.sort ( key=lambda acc: acc['name'] )  
  
  # now accs contains list of expense accounts we are interested in 
  
  out ( "MONTHLY BUDGET" )
  outln ()
  iv = ivlMonthly()
  plan ( accs, start, budgetSince, end, iv, 1 )
  
  out ( "SEMIMONTHLY BUDGET" )
  outln ()
  iv = ivlSemimonthly()
  plan ( accs, start, budgetSince, end, iv, 1 )
  
  out ( "WEEKLY BUDGET" )
  outln ()
  iv = ivlWeekly()
  plan ( accs, start, budgetSince, end, iv, 0 )


# Budgeting plan
# CFD(h,x) - discrete distribution function built of discrete historical samples of an account 'h'.
# 'x' is the amount of profit/loss in the interval
# EX: samples are: h = [33, 36, 38, 44]
#   CFD(h,32) = 0
#   CFD(h,33) = 0.25
#   CFD(h,35.9) = 0.25
#   CFD(h,36) = 0.5, etc
# CFDa(h,x,w) - averaged distribution function
# 'w' - running average width, can be taken as (max(h)-min(h))/2
# CFDa(h,x,w) = INT ( CFDr(h,i)/w, i= x-w .. x+w )
# C(h,x,w,r) - response adjusted distribution
# 'r' - correction response rate of the account, [0..1], 1 - full response, 0 - no response
# C(h,x,w,r) = ( CFDa(h,x,w) - CFDa(h,0.5,w) )*r + CFDa(h,0.5,w) = CFDa(h,x,w)*r + CFDa(h,0.5,w)*(1-r)

