use leesdata;
ALTER TABLE idl_datasource_count_ft DROP PARTITION(tablename='adl_limao_recommended_cid_agg',ds='{p0}');
insert into table idl_datasource_count_ft partition(tablename, ds)
select 
    'all_count' as colname, 
    count(1) as data_count,
    'adl_limao_recommended_cid_agg' as tablename,
    ds
from 
    adl_limao_recommended_cid_agg
where ds = '{p0}'
group by ds;