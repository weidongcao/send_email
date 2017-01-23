--idl_moblie_name_agg表
insert into idl_moblie_name_ft partition(ds = '2017-01-08')
select
    mobile_province as mobile_province,     --省
    mobile_city as mobile_city,     --市
    mobile_operators as mobile_operators,   --移动运营商
    count(distinct mobile_no) as total_num,     --号码总数(去重)
    round(avg(name_num), 2) as name_num_avg,    --小数点后保留两位
    round(avg(max_relation), 2) as max_relation_avg     --小数点后保留两位
from
    idl_moblie_name_agg 
where
    ds = '2017-01-08'
group by 
    --根据省, 市, 运营商进行分组
    mobile_province, mobile_city, mobile_operators;