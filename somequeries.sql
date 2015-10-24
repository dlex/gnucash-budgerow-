select ap.name||'/'||a.name, * from accounts a 
	join accounts ap on ap.guid=a.parent_guid
	where a.guid='ea5050080e86ad7fbf26e62a0d3f7a5a';

select distinct reconcile_state from splits;
select distinct value_denom from splits;
select * from splits s where s.account_guid='ea5050080e86ad7fbf26e62a0d3f7a5a';
select * from transactions t join splits s on s.tx_guid=t.guid 
	where s.account_guid='ea5050080e86ad7fbf26e62a0d3f7a5a'
	and reconcile_state in ('y','c')
	order by t.post_date;
-- current cleared amount
select sum(cast(value_num as numeric(10,2))/100.0) from transactions t join splits s on s.tx_guid=t.guid 
	where s.account_guid='ea5050080e86ad7fbf26e62a0d3f7a5a'
	and reconcile_state in ('y','c');