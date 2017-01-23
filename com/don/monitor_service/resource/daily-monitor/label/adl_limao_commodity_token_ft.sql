--统计adl_limao_commodity_token_agg标签
insert into adl_limao_commodity_token_ft partition(ds = '2017-01-08')
select
    info.token_key as key_word,     --标签名
    count(distinct info.mobile_no) as mobile_num,   --统计电话号码
    round(avg(if(info.token_value > 100, 100, info.token_value)), 2) as tid_avg     --标签平均值(如果值大于100按100算),round取小数点后两位
from
    (select 
        token.token_key as token_key,   --标签名
        commodity.mobile_no as mobile_no,   --手机号码
        cast(token.token_value as float) as token_value,    --标签值
        commodity.ds as ds    --日期
    from 
        adl_limao_commodity_token_agg commodity     --所要统计的表
    --将标签所在的字段(类型map<string,string>)进行切分并行转列, key命名为token_key, value命名为token_value
    lateral view explode(commodity.token_t) token as token_key, token_value
    where
        commodity.ds = '2017-01-08'
    ) as info
where
    info.ds = '2017-01-08'    --这个是出于Hive优化的考虑,group by不加条件的话只会有一个reduce
group by 
    --根据标签名进行分组
    info.token_key;     
