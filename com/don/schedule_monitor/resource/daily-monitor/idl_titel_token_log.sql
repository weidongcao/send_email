use leesdata;
ALTER TABLE idl_datasource_count_ft DROP PARTITION(tablename='idl_titel_token_log',ds='{p0}');
insert into table idl_datasource_count_ft partition(tablename, ds)
select 
    'all_count' as colname, 
    count(1) as data_count,
    'idl_titel_token_log' as tablename,
    ds
from 
    idl_titel_token_log
where ds = '{p0}'
group by ds;