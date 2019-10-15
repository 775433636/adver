#!/bin/bash

hive=/opt/apps/hive-1.2.1-bin/bin/hive

if [ -n "$1" ] ; then
    do_date=$1
else
    do_date=`date -d "-1 day" +%F`
fi

sql="


insert into table dwd_adver.dwd_bidding_success_session 
partition (bdp_day='$do_date')
select 
a.release_session
from
(
    select
    release_session,
    get_json_object(exts,'$.bidding_status') bidding_status,
    get_json_object(exts,'$.bidding_type') bidding_type
    from
    ods_adver.ods_adver
    where 
    bdp_day='$do_date' and release_status=2
) a
where bidding_type='RTB' and a.bidding_status=2 or bidding_type='PMP';



insert into table dwd_adver.dwd_not_gain_client
partition (bdp_day='$do_date')
select
release_session,
release_status,
device_num,
device_type,
sources,
channels,
get_json_object(exts,'$.idcard') idcard,
myage(from_unixtime(unix_timestamp(substring(get_json_object(exts,'$.idcard'),7,8),'yyyyMMdd'),'yyyy-MM-dd')) age,
case when substring(get_json_object(exts,'$.idcard'),17,1)%2=0 then 'woman' else 'man' end gender,
get_json_object(exts,'$.area_code') area_code,
round(get_json_object(exts,'$.longitude'),2) longitude,
round(get_json_object(exts,'$.latitude'),2) latitude,
get_json_object(exts,'$.matter_id') matter_id,
get_json_object(exts,'$.model_code') model_code,
get_json_object(exts,'$.model_version') model_version,
get_json_object(exts,'$.aid') aid,
ct
from
ods_adver.ods_adver
where 
release_status = 0 and bdp_day='$do_date'
;



insert into table dwd_adver.dwd_gain_client
partition (bdp_day='$do_date')
select
release_session,
release_status,
device_num,
device_type,
sources,
channels,
get_json_object(exts,'$.idcard') idcard,
myage(from_unixtime(unix_timestamp(substring(get_json_object(exts,'$.idcard'),7,8),'yyyyMMdd'),'yyyy-MM-dd')) age,
case when substring(get_json_object(exts,'$.idcard'),17,1)%2=0 then 'woman' else 'man' end gender,
get_json_object(exts,'$.area_code') area_code,
round(get_json_object(exts,'$.longitude'),2) longitude,
round(get_json_object(exts,'$.latitude'),2) latitude,
get_json_object(exts,'$.matter_id') matter_id,
get_json_object(exts,'$.model_code') model_code,
get_json_object(exts,'$.model_version') model_version,
get_json_object(exts,'$.aid') aid,
ct
from
ods_adver.ods_adver 
where 
release_status = 1 and bdp_day='$do_date'
;


insert into table dwd_adver.dwd_gain_client_user_success_bidding
partition (bdp_day='$do_date')
select 
  a.release_session ,
  a.release_status,
  a.device_num ,
  a.device_type ,
  a.sources ,
  a.channels ,
  a.idcard ,
  a.age ,
  a.gender,
  a.area_code ,
  a.longitude ,
  a.latitude,
  a.matter_id ,
  a.model_code ,
  a.model_version ,
  a.aid ,
  a.ct
from
dwd_adver.dwd_gain_client a
where 
a.bdp_day='$do_date'
and exists (select 1 from dwd_adver.dwd_bidding_success_session b where bdp_day='$do_date' and a.release_session=b.success_session)
;


insert into table dwd_adver.dwd_bidding_success
partition (bdp_day='$do_date')
select
a.release_session,
a.release_status,
get_json_object(a.exts,'$.bidding_status') bidding_status,
get_json_object(a.exts,'$.bidding_type') bidding_type,
get_json_object(a.exts,'$.bidding_price') bidding_price,
get_json_object(a.exts,'$.aid') aid
from
ods_adver.ods_adver a
where 
release_status = 2 and bdp_day='$do_date'
and exists (select 1 from dwd_adver.dwd_bidding_success_session b where bdp_day='$do_date' and  a.release_session=b.success_session)
;


insert into table dwd_adver.dwd_exposure_bidding_success
partition (bdp_day='$do_date')
select
a.release_session,
a.release_status
from
ods_adver.ods_adver a
where 
release_status = 3 and bdp_day='$do_date'
and exists (select 1 from dwd_adver.dwd_bidding_success_session b where bdp_day='$do_date' and  a.release_session=b.success_session)
;


insert into table dwd_adver.dwd_click_bidding_success
partition (bdp_day='$do_date')
select
a.release_session,
a.release_status
from
ods_adver.ods_adver a
where 
release_status = 4 and bdp_day='$do_date'
and exists (select 1 from dwd_adver.dwd_bidding_success_session b where bdp_day='$do_date' and  a.release_session=b.success_session)
;


insert into table dwd_adver.dwd_arrive_bidding_success
partition (bdp_day='$do_date')
select
a.release_session,
a.release_status
from
ods_adver.ods_adver a
where 
release_status = 5 and bdp_day='$do_date'
and exists (select 1 from dwd_adver.dwd_bidding_success_session b where bdp_day='$do_date' and  a.release_session=b.success_session)
;


insert into table dwd_adver.dwd_register_bidding_success
partition (bdp_day='$do_date')
select
a.release_session,
a.release_status,
get_json_object(a.exts,'$.user_register') user_register
from
ods_adver.ods_adver a
where 
release_status = 6 and bdp_day='$do_date'
and exists (select 1 from dwd_adver.dwd_bidding_success_session b where bdp_day='$do_date' and  a.release_session=b.success_session)
;

insert into table dws_adver.dws_bidding_success_adver_wide
partition (bdp_day='$do_date')
select 
  a.release_session ,
  a.release_status ,
  a.device_num ,
  a.device_type ,
  a.sources ,
  a.channels,
  a.idcard ,
  a.age ,
  a.gender ,
  a.area_code ,
  a.longitude,
  a.latitude ,
  a.matter_id ,
  a.model_code,
  a.model_version ,
  a.aid,
  a.ct,
  b.release_status b_release_status,
  b.bidding_status,
  b.bidding_type,
  b.bidding_price ,
  b.aid ,
  c.release_status c_release_status,
  d.release_status d_release_status,
  e.release_status e_release_status,
  f.release_status f_release_status,
  f.user_register user_register
from
dwd_adver.dwd_gain_client_user_success_bidding a
left join 
dwd_adver.dwd_bidding_success b
on a.release_session=b.release_session and b.bdp_day='$do_date'
left join 
dwd_adver.dwd_exposure_bidding_success c 
on a.release_session=c.release_session and c.bdp_day='$do_date'
left join
dwd_adver.dwd_click_bidding_success d
on a.release_session=d.release_session and d.bdp_day='$do_date'
left join
dwd_adver.dwd_arrive_bidding_success e
on a.release_session=e.release_session and e.bdp_day='$do_date'
left join 
dwd_adver.dwd_register_bidding_success f
on a.release_session=f.release_session and f.bdp_day='$do_date'
where 
a.bdp_day='$do_date'
;

"


hive -e "$sql"