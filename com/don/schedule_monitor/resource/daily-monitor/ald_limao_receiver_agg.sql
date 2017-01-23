use leesdata;
ALTER TABLE idl_datasource_count_ft DROP PARTITION(tablename='ald_limao_receiver_agg',ds='{p0}');
insert into table idl_datasource_count_ft partition(tablename, ds)
select 
    'all_count' as colname, 
    count(1) as data_count,
    'ald_limao_receiver_agg' as tablename,
    ds
from 
    ald_limao_receiver_agg
where ds = '{p0}'
group by ds;