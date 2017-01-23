--odl_limao_order_logs表
insert into idl_limao_order_ft partition(ds = '2017-01-08')
select
    count(1) as total_num,      --总量
    sum(case when
        substr(modified, 1, 10) =  substr(created, 1, 10)
        then 1 else 0 
    end) as new_num
from
    odl_limao_order_logs
where
    ds like '2017-01-08%'
group by 
    --根据标签名进行分组
    ds;