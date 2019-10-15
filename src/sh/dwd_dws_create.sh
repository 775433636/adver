#!/bin/bash

hive=/opt/apps/hive-1.2.1-bin/bin/hive


sql="
create database if not exists dwd_adver;
create database if not exists dws_adver;


create external table if not exists dwd_adver.dwd_bidding_success_session(

success_session string comment '竞价成功的'

)
partitioned by (bdp_day string)
;

create external table if not exists dwd_adver.dwd_not_gain_client(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
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
  aid string comment '广告位id',
  ct bigint comment '创建时间'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_gain_client(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
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
  aid string comment '广告位id',
  ct bigint comment '创建时间'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_gain_client_user_success_bidding(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
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
  aid string comment '广告位id',
  ct bigint comment '创建时间'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_bidding_success
(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
  bidding_status string comment '竞价阶段',
  bidding_type string comment '竞价类型',
  bidding_price string comment '出价',
  aid string comment '广告位id'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_exposure_bidding_success
(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_click_bidding_success
(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_arrive_bidding_success
(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明'
)
partitioned by (bdp_day string)
stored as parquet;


create external table if not exists dwd_adver.dwd_register_bidding_success
(
  release_session string comment '投放会话id',
  release_status string comment '参考下面投放流程状态说明',
  user_register string comment '手机号'
)
partitioned by (bdp_day string)
stored as parquet;



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
partitioned by (bdp_day string)
stored as parquet;


"


hive -e "$sql"