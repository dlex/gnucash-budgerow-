drop table bw_accounts;
create table bw_accounts (
	guid text(32) primary key not null,
	participation integer not null default 1, -- 0 for no participation in budgeting, not inherited
	history_start text(14) -- inherited, not null overrides parent
	);
	
