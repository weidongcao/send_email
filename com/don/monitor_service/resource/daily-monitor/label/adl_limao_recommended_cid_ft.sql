--统计adl_limao_recommended_cid_agg标签
insert into adl_limao_recommended_cid_ft partition(ds = '2017-01-08')
select
    split(info.cobj_key, "&")[0] as item_name,  --标签名1
    split(info.cobj_key, "&")[1] as suritem_name,   --标签名2
    count(distinct info.mobile_no) as mobile_num,   --统计手机号码
    round(avg(if(info.cobj_value > 100, 100, info.cobj_value)), 2) as cobj_avg      --标签平均值(如果值大于100按100算),round取小数点后两位
from
    (select 
        cobj.cobj_key as cobj_key,  --标签名
        recommended.mobile_no as mobile_no,     --手机号码
        cast(cobj.cobj_value as float) as cobj_value,   --标签值
        recommended.ds as ds    --日期
    from 
        adl_limao_recommended_cid_agg recommended       --所要统计的表
    --将标签所在的字段(类型map<string,string>)进行切分并行转列, key命名为cobj_key, value命名为cobj_value
    lateral view explode(recommended.cobj_list) cobj as cobj_key, cobj_value    
    where
        recommended.ds = '2017-01-08'
    ) as info
where
    info.ds = '2017-01-08'    --这个是出于Hive优化的考虑,group by不加条件的话只会有一个reduce
group by 
    --根据标签名进行分组
    info.cobj_key;