--统计ald_limao_receiver_agg
insert into ald_limao_receiver_ft partition(ds = '2017-01-08')
select
    count(1) as total_num,      --总量
    round(avg(weigth_total),2) as weigth_avg,
    round(avg(receiver_total),2) as tag_num_avg
from
    ald_limao_receiver_agg
where
    ds = '2017-01-08'
group by 
    --根据标签名进行分组
    ds;
