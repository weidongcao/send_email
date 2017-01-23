--idl_limao_info_dim表邮箱
insert into idl_limao_info_ft partition(ds = '2017-01-08')
select 
    split(elist.email_item, "@")[1] as email_type,      --邮箱类型如qq邮箱,谷歌邮箱,@后面为邮箱类型
    count(distinct elist.email_item) as email_num       --指定邮箱类型下邮箱总数
from 
    idl_limao_info_dim dim 
    -- 将email 数组行转列
    lateral view explode(dim.email) elist as email_item 
where 
    ds = '2017-01-08'
    and locate("@", elist.email_item) > 0       --判断邮箱是否非法
group by 
    split(elist.email_item, "@")[1];
