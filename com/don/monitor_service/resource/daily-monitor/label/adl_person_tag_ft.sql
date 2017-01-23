--统计adl_person_tag_agg标签
insert into adl_person_tag_ft partition(ds = '2017-01-08')
select 
    info.psb_key as tag_name,  --标签名
    count(distinct info.mobile_no) as mobile_num,   --统计手机号码
    round(avg(if(info.psb_value > 100, 100, info.psb_value)), 2) as tag_psb_avg,    --平均标签值
    avg(info.tag_num) as tag_num_avg    --平均标签量
from 
    (select 
        psb.psb_key,        --标签名
        tag.mobile_no,      --手机号码
        psb.psb_value,      --标签值
        tag.tag_num,        --标签量
        tag.ds
    from 
        adl_person_tag_agg  tag     --所要统计的表
    --将标签所在的字段(类型map<string,string>)进行切分并行转列, key命名为psb_key, value命名为psb_value
    lateral view explode(tag.tag_psb) psb as psb_key, psb_value
    where
        ds = '2017-01-08') as info
where
    ds = '2017-01-08'     --这个是出于Hive优化的考虑,group by不加条件的话只会有一个reduce
group by 
    --根据标签名进行分组
    info.psb_key;