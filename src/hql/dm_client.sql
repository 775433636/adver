create external table if not exists dws_adver.dws_bidding_success_adver_wide(
  release_session string comment '投放会话id',
  a_release_status string comment '参考下面投放流程状态说明',
  device_num string comment '设备唯一编码',
  device_type string comment '1 android| 2 ios | 9 其他',
  sources string comment '渠道',
  channels string comment '通道',
  idcard string comment '身份证',
  age int comment '年龄',
  gender string comment '性别',
  area_code string comment '地区',
  longitude string comment '经度',
  latitude string comment '纬度',
  matter_id string comment '物料代码',
  model_code string comment '模型代码',
  model_version string comment '模型版本',
  a_aid string comment '广告位id',
  ct bigint comment '创建时间' ,
  b_release_status string comment '参考下面投放流程状态说明',
  bidding_status string comment '竞价阶段',
  bidding_type string comment '竞价类型',
  bidding_price string comment '出价',
  b_aid string comment '广告位id',
  c_release_status string comment '参考下面投放流程状态说明' ,
  d_release_status string comment '参考下面投放流程状态说明' ,
  e_release_status string comment '参考下面投放流程状态说明' ,
  f_release_status string comment '参考下面投放流程状态说明' ,
  user_register string comment '手机号'
)
stored as parquet;

#1.
#pv
#每个特定广告的点击数
select
b_aid,
count(*) `aid_PV`
from
dws_adver.dws_bidding_success_adver_wide
where
e_release_status is not null
group by b_aid


#2. 
#adpv
#某特定广告位上有广告点击并注册数量
select 
b_aid,
count(*) `regionNum`
from
dws_adver.dws_bidding_success_adver_wide
where
f_release_status is not null
group by b_aid


#3.
#PVR
#adpv/pv


select
t1.b_aid,
t1.aid_PV,
t2.regionNum,
round(t2.regionNum/t1.aid_PV*100)
from
    (
    select
    b_aid,
    count(*) `aid_PV`
    from
    dws_adver.dws_bidding_success_adver_wide
    where
    e_release_status is not null
    group by b_aid
    )t1
left join
    (
    select 
    b_aid,
    count(*) `regionNum`
    from
    dws_adver.dws_bidding_success_adver_wide
    where
    f_release_status is not null
    group by b_aid
    )t2
on t1.b_aid=t2.b_aid



#4. pv adpv pvr
select 
t1.aid,
t1.sum_pv,
t2.adpv,
round(t2.adpv/t1.sum_pv*100 , 2)
from
    (
    select 
    aid,
    sum(t.pv) sum_pv
    from
        (
        select 
        aid,
        count(*)as pv
        from
        dwd_adver.dwd_gain_client
        group by 
        aid
        union all
        select 
        aid,
        count(*)as pv
        from
        dwd_adver.dwd_not_gain_client
        group by 
        aid
        ) t
    group by t.aid
    )t1
    left join 
    (
    select 
    b_aid,
    count(*) adpv
    from
    dws_adver.dws_bidding_success_adver_wide
    group by b_aid
    )t2
    on t1.aid=t2.b_aid

0801    1571    1132    72.0
0802    1610    1171    73.0
1001    1568    1106    71.0
1002    1662    1182    71.0
1003    1589    1138    72.0

#cost
select 
b_aid,
sum(nvl(bidding_price,0)) cost
from
dws_adver.dws_bidding_success_adver_wide
group by b_aid


#CLK
select 
t.b_aid,
round(t.d/t.c*100,0)
from
(
    select 
    b_aid,
    sum(case when d_release_status is not null then 1 else 0 end) d,
    sum(case when c_release_status is not null then 1 else 0 end) c
    from
    dws_adver.dws_bidding_success_adver_wide
    group by b_aid
)t



#ACP

select 
t1.b_aid,
t1.cost,
t2.clk,
round(t1.cost/t2.clk,2) acp
from
    (
    select 
    b_aid,
    sum(nvl(bidding_price,0)) cost
    from
    dws_adver.dws_bidding_success_adver_wide
    group by b_aid
    )t1
left join 
    (
    select 
    t.b_aid,
    round(t.d/t.c*100,2) clk
    from
    (
        select 
        b_aid,
        sum(case when d_release_status is not null then 1 else 0 end) d,
        sum(case when c_release_status is not null then 1 else 0 end) c
        from
        dws_adver.dws_bidding_success_adver_wide
        group by b_aid
    )t
    )t2
    on t1.b_aid=t2.b_aid



select 
    t.b_aid,
    t.cost,
    round(t.d/t.c*100,2) clk ,
    round(t.cost/round(t.d/t.c*100,2),2) acp
from
    (
    select 
    b_aid,
    sum(nvl(bidding_price,0)) cost,
    sum(case when d_release_status is not null then 1 else 0 end) d,
    sum(case when c_release_status is not null then 1 else 0 end) c
    from
    dws_adver.dws_bidding_success_adver_wide
    group by b_aid
    )t
 
0801    1290.0  75.52   17.08
0802    1342.0  72.53   18.5
1001    1267.0  70.73   17.91
1002    1371.0  73.81   18.57
1003    1235.0  72.31   17.08
 
 
 
 
 
#crt1


select 
round(t2.client_num/t1.cou_pro,2) crt1,
round(t2.client_num/t2.exposure_num,2) crt2
from
    (
    select 
    count(*) cou_pro
    from
    dwd_adver.dwd_gain_client
    )t1
,
    (
    select 
    sum(case when d_release_status is not null then 1 else 0 end) client_num,
    sum(case when c_release_status is not null then 1 else 0 end) exposure_num
    from
    dws_adver.dws_bidding_success_adver_wide
    )t2

crt1    crt2
0.49    0.73


select 
    t.b_aid,
    t.cost,
    round(t.d/t.c*100,2) clk ,
    round(t.cost/round(t.d/t.c*100,2),2) acp,
    round(t.client_num/t1.cou_pro,2) crt1,
    round(t.client_num/t.exposure_num,2) crt2
from
    (
    select 
    b_aid,
    sum(nvl(bidding_price,0)) cost,
    sum(case when d_release_status is not null then 1 else 0 end) d,
    sum(case when c_release_status is not null then 1 else 0 end) c,
    sum(case when d_release_status is not null then 1 else 0 end) client_num,
    sum(case when c_release_status is not null then 1 else 0 end) exposure_num
    from
    dws_adver.dws_bidding_success_adver_wide
    group by b_aid
    )t
    ,
    (
    select 
    count(*) cou_pro
    from
    dwd_adver.dwd_gain_client
    )t1




select 
round(t2.client_num/t1.cou_pro,2) crt1,
round(t2.client_num/t2.exposure_num,2) crt2
from
    (
    select 
    count(*) cou_pro
    from
    dwd_adver.dwd_gain_client
    )t1
,
    (
    select 
    sum(case when d_release_status is not null then 1 else 0 end) client_num,
    sum(case when c_release_status is not null then 1 else 0 end) exposure_num
    from
    dws_adver.dws_bidding_success_adver_wide
    )t2



#pv adpv pvr as asn cost clk acp cpm1 cpm2  三层转化率，曝光到注册

select 
t1.aid,
t1.sum_pv,
t2.adpv,
round(t2.adpv/t1.sum_pv*100 , 2) pvr ,
t2.c `as`,
round(t2.c/t2.adpv,2) asn,
t2.cost,
round(t2.d/t2.c*100,2) clk ,
round(t2.cost/round(t2.d/t2.c*100,2),2) acp,
round(t2.client_num/t1.gain_pv,2) crt1,
round(t2.client_num/t2.exposure_num,2) crt2,
round(t2.cost*1000/t1.sum_pv,2) cpm1,
round(t2.cost*1000/t2.c,2) cpm2 ,
round(t2.client_num/exposure_num,2) c_d,
round(t2.arrive_num/client_num,2) d_e,
round(t2.register_num/arrive_num,2) e_f,
round(t2.register_num/exposure_num,2) c_f
from
    (
    select 
    aid,
    sum(t.sum_pv) sum_pv,
    sum(t.gain_pv) gain_pv
    from
        (
        select 
        aid,
        count(*)as sum_pv,
        count(*)as gain_pv
        from
        dwd_adver.dwd_gain_client
        group by 
        aid
        union all
        select 
        aid,
        count(*)as not_gain_pv,
        '0'
        from
        dwd_adver.dwd_not_gain_client
        group by 
        aid
        ) t
    group by t.aid
    )t1
    left join 
    (
    select 
    b_aid,
    count(*) adpv,
    sum(nvl(bidding_price,0)) cost,
    sum(case when d_release_status is not null then 1 else 0 end) d,
    sum(case when c_release_status is not null then 1 else 0 end) c,
    sum(case when d_release_status is not null then 1 else 0 end) client_num,
    sum(case when c_release_status is not null then 1 else 0 end) exposure_num,
    sum(case when e_release_status is not null then 1 else 0 end) arrive_num,
    sum(case when f_release_status is not null then 1 else 0 end) register_num
    from
    dws_adver.dws_bidding_success_adver_wide
    group by b_aid
    )t2
    on t1.aid=t2.b_aid


#转化率，点击用户的平均付费金额，每点击成本,DAU
select 
round(t.all_click/t.all_exposure,2) c_d ,
round(t.all_arrive/t.all_click,2) d_e,
round(t.all_register/t.all_arrive,2) e_f,
round(t.click_1day/exposure_1day,2) c_d1,
round(t.arrive_1day/click_1day,2) d_e1,
round(t.register_1day/t.arrive_1day,2) e_f1,
round(t.all_click/sum_price) click_priceavg_all,
round(t.click_1day/sum_price_1day) click_priceavg_1day,
round(sum_price/t.all_click) cpa_all,
round(sum_price_1day/t.click_1day) cpa_1day,
t.DAU,
t.keep_1day,
t.keep_2day,
t.keep_3day,
t.keep_4day,
t.keep_5day,
t.keep_6day,
t.keep_7day
from
    (
    select 
    sum(case when c_release_status is not null then 1 else 0 end) all_exposure,
    sum(case when d_release_status is not null then 1 else 0 end) all_click,
    sum(case when e_release_status is not null then 1 else 0 end) all_arrive,
    sum(case when f_release_status is not null then 1 else 0 end) all_register,
    sum(case when c_release_status is not null and bdp_day='20190731' then 1 else 0 end) exposure_1day,
    sum(case when d_release_status is not null and bdp_day='20190731' then 1 else 0 end) click_1day,
    sum(case when e_release_status is not null and bdp_day='20190731' then 1 else 0 end) arrive_1day,
    sum(case when f_release_status is not null and bdp_day='20190731' then 1 else 0 end) register_1day,
    count(distinct(case when e_release_status is not null and bdp_day='20190731' then device_num else null end)) DAU,
    sum(bidding_price) sum_price,
    sum(case when bdp_day='20190731' then bidding_price else 0 end ) sum_price_1day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-1) and e_release_status is not null then 1 else 0 end) keep_1day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-2) and e_release_status is not null then 1 else 0 end) keep_2day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-3) and e_release_status is not null then 1 else 0 end) keep_3day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-4) and e_release_status is not null then 1 else 0 end) keep_4day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-5) and e_release_status is not null then 1 else 0 end) keep_5day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-6) and e_release_status is not null then 1 else 0 end) keep_6day,
    sum(case when from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd')=date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-7) and e_release_status is not null then 1 else 0 end) keep_7day
    from
    dws_adver.dws_bidding_success_adver_wide
    )t

c_d     d_e     e_f     c_d1    d_e1    e_f1    click_priceavg_all      click_priceavg_1day      cpc_all cpc_1day
0.73    0.69    0.51    0.76    0.72    0.58    1.0     1.0     2.0     2.0



select
round(t.click_age15to25/exposure_age15to25,2) exposure2click_age15to25,
round(t.click_age26to35/exposure_age26to35,2) exposure2click_age26to35,
round(t.click_age36to50/exposure_age36to50,2) exposure2click_age36to50,
round(t.click_cou/t.exposure_cou,2) CTR
from
(
select 
sum(case when age>=15 and age<=25 and c_release_status is not null then 1 else 0 end) exposure_age15to25,
sum(case when age>=26 and age<=35 and c_release_status is not null then 1 else 0 end) exposure_age26to35,
sum(case when age>=36 and age<=50 and c_release_status is not null then 1 else 0 end) exposure_age36to50,
sum(case when age>=15 and age<=25 and d_release_status is not null then 1 else 0 end) click_age15to25,
sum(case when age>=26 and age<=35 and d_release_status is not null then 1 else 0 end) click_age26to35,
sum(case when age>=36 and age<=50 and d_release_status is not null then 1 else 0 end) click_age36to50,
sum(case when c_release_status is not null then 1 else 0 end) exposure_cou,
sum(case when d_release_status is not null then 1 else 0 end) click_cou
from
dws_adver.dws_bidding_success_adver_wide
)t



select 
t.device_num,
count(t.rank) continuous_day 
from
(
select 
device_num,
date_sub(from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd'),row_number() over(partition by device_num order by bdp_day desc)) rank,
count()
from
dws_adver.dws_bidding_success_adver_wide
where bdp_day>=20190725 and bdp_day<=20190731 and c_release_status is not null
)t
group by t.device_num,t.rank
having continuous_day>=2




#最近一周点击超过四次的
select 
device_num,
count(*) week_client_cou
from
dws_adver.dws_bidding_success_adver_wide
where bdp_day>=20190725 and bdp_day<=20190731 and d_release_status is not null
group by device_num
having week_client_cou>= 4



select
device_num,
count(*)
from
dws_adver.dws_bidding_success_adver_wide
where f_release_status is not null
group by device_num
having count(*) >= 2 

select 
count(distinct(device_num))
from
dws_adver.dws_bidding_success_adver_wide
where 
bdp_day='20190731' and e_release_status is not null




select 
count(distinct(t.arrive))
from
(
select
count(distinct(case when e_release_status is not null and bdp_day='20190731' then device_num else null end)) arrive
from
dws_adver.dws_bidding_success_adver_wide
)t


#七日留存率

select 
*
from
(
select 
device_num keep_7
from
dws_adver.dws_bidding_success_adver_wide
where 
e_release_status is not null and bdp_day=from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp('20190731','yyyyMMdd'),'yyyy-MM-dd'),-6),'yyyy-MM-dd'),'yyyyMMdd')
)t1
left join
(
select 
device_num now
from 
dws_adver.dws_bidding_success_adver_wide
where 
e_release_status is not null and bdp_day='201907'
)t2
on t1.keep_7=t2.now





##沉默

select 
count(t1.device_num) cou,
count(t2.a)
from
(
select 
device_num
from
dws_adver.dws_bidding_success_adver_wide
where e_release_status is not null and bdp_day='20190725'
)t1
left join
(
select 
device_num,
count(*) a
from
dws_adver.dws_bidding_success_adver_wide
where e_release_status is not null and bdp_day>='20190725' and bdp_day<='20190731'
group by device_num
)t2
on t1.device_num=t2.device_num and t2.a=1


select 
a.device_num
from
dws_adver.dws_bidding_success_adver_wide a
left join 
dws_adver.dws_bidding_success_adver_wide b
on a.device_num=b.device_num and a.bdp_day='20190731' and b.bdp_day='20190730'
left join 
where b.device_num is null 



select 
count(t1.device_num) cou,
count(t2.a)
from
(
select 
device_num
from
dws_adver.dws_bidding_success_adver_wide
where e_release_status is not null and bdp_day='20190725'
)t1
left join
(
select 
device_num,
count(*) a
from
dws_adver.dws_bidding_success_adver_wide
where e_release_status is not null and bdp_day>='20190725' and bdp_day<='20190731'
group by device_num
)t2
on t1.device_num=t2.device_num and t2.a=0





SELECT 
count(*)
FROM
dws_adver.dws_bidding_success_adver_wide
where bdp_day>='20190728' and bdp_day<='20190731'
group by device_num
having count(*) >= 3






####最近连续3周活跃用户数

select 
count(*)
from
dws_adver.dws_bidding_success_adver_wide a
left join 
dws_adver.dws_bidding_success_adver_wide b
on a.device_num=b.device_num and b.bdp_day='20190729' and b.e_release_status is not null
left join 
dws_adver.dws_bidding_success_adver_wide c
on a.device_num=c.device_num and c.bdp_day='201907230' and c.e_release_status is not null
where b.device_num is not null and c.device_num is not null and a.e_release_status is not null




活跃用户表
select 
distinct(device_num) arrive,
bdp_day
from 
dws_adver.dws_bidding_success_adver_wide
where 
e_release_status is not null

with t as 
(
select 
distinct(device_num) arrive,
e_release_status,
bdp_day
from 
dws_adver.dws_bidding_success_adver_wide
)
select 
arrive,
count(*)
from
t
where 
t.e_release_status is not null and t.bdp_day>='20190728' and t.bdp_day>='20190731'
group by t.arrive
having count(*)>=3



#####最近七天内连续三天活跃用户数


with t as 
(
select 
distinct(device_num) arrive,
e_release_status,
bdp_day
from 
dws_adver.dws_bidding_success_adver_wide
)
,
t1 as
(
select 
t.arrive,
t.bdp_day,
date_add(from_unixtime(unix_timestamp(t.bdp_day,'yyyyMMdd'),'yyyy-MM-dd'),row_number() over(partition by t.arrive order by t.bdp_day desc)) rank_date
from
t
where 
t.e_release_status is not null and t.bdp_day>='20190725' and t.bdp_day>='20190731'
)
,
t2 as
(
select 
arrive,
count(rank_date) ranka
from
t1
group by arrive , rank_date
)
select 
arrive,
max(ranka) max_list
from
t2
group by arrive
having max_list>=3





##不同渠道的重复到达率

with t as
(
select 
if(count(*)=2 , 1 , 0) arrive2,
count(*) cou_arrive,
sources
from
dws_adver.dws_bidding_success_adver_wide
where
e_release_status is not null and bdp_day>='20190725' and bdp_day>='20190731'
group by 
sources , device_num
)
select 
t.sources,
round(sum(t.arrive2)/sum(t.cou_arrive),2) repetition_arrive
from
t
group by t.sources



##假设前一张为


(
select 
t.*
from
(
select 
release_session,
'付费' type,
from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd') datee,
'9999-99-99' enddate
from
dws_adver.dws_bidding_success_adver_wide
where bdp_day='20190725'
union all
select 
t1.release_session,
t1.c_release_status,
from_unixtime(unix_timestamp(t1.bdp_day,'yyyyMMdd'),'yyyy-MM-dd') datee,
t2.enddate
from
dws_adver.dws_bidding_success_adver_wide t1
left join
    (
    select 
    release_session,
    c_release_status,
    from_unixtime(unix_timestamp(bdp_day,'yyyyMMdd'),'yyyy-MM-dd') datee,
    '9999-99-99' enddate
    from
    dws_adver.dws_bidding_success_adver_wide
    )t2
on t1.release_session=t2.release_session and t1.c_release_status is not null and t2.c_release_status is not null and enddate='9999-99-99' and t1.bdp_day='20190725' and t2.datee='2019-07-25'
where t2.c_release_status is not null
)t
order by t.release_session
)t3

