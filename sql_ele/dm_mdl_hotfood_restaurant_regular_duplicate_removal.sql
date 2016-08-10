#***************************************************************************************************
# **  文件名称： dm_mdl_hotfood_restaurant_regular_duplicate_removal.sql
# **  功能描述： 餐厅名称正则去重映射
# **  创建者： jiahao.dong
# **  创建日期： 2016-08-01
#***************************************************************************************************


drop table temp.temp_restaurant_seg_name_mapping_1;
create table temp.temp_restaurant_seg_name_mapping_1 as
select name, single_seg
from(
select name, segmenter(name,true,'food_name.dic','stop.dict') as seg_arr
from dw.dw_prd_restaurant 
where dt='2016-08-03' 
) t 
lateral view explode(seg_arr) tmp_table as single_seg;

drop table temp.temp_restaurant_seg_name_mapping_2;
create table temp.temp_restaurant_seg_name_mapping_2 as
select single_seg, count(distinct name) as frq
from temp.temp_restaurant_seg_name_mapping_1
group by single_seg
sort by frq desc;

DROP TABLE temp.temp_restaurant_seg_name_mapping_3;
CREATE TABLE temp.temp_restaurant_seg_name_mapping_3(
seg_name STRING, 
priority DOUBLE
) PARTITIONED BY (part STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/seg_frq' OVERWRITE INTO TABLE  temp.temp_restaurant_seg_name_mapping_3 PARTITION (part='base');

DROP TABLE temp.temp_restaurant_seg_name_mapping_4;
CREATE TABLE temp.temp_restaurant_seg_name_mapping_4(
name STRING,
seg_name STRING, 
priority DOUBLE
) PARTITIONED BY (part STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/res_name' OVERWRITE INTO TABLE  temp.temp_restaurant_seg_name_mapping_4 PARTITION (part='base');


drop table temp.temp_restaurant_seg_name_mapping_5;
create table temp.temp_restaurant_seg_name_mapping_5 as
select t1.name, t1.single_seg, t2.priority
from(select name, single_seg from temp.temp_restaurant_seg_name_mapping_1) t1
join(select seg_name, priority from temp.temp_restaurant_seg_name_mapping_3 where part is not null) t2
on t1.single_seg=t2.seg_name
union all
select name, seg_name as single_seg, priority
from temp.temp_restaurant_seg_name_mapping_4
where part is not null;


drop table temp.temp_restaurant_seg_name_mapping_6;
create table temp.temp_restaurant_seg_name_mapping_6 as
select t1.name, t2.single_seg
from (select name, max(priority) as max_priority from temp.temp_restaurant_seg_name_mapping_5 group by name) t1
join (select name, single_seg, priority from temp.temp_restaurant_seg_name_mapping_5) t2
on t1.name=t2.name and t1.max_priority=t2.priority;

-- drop table dm.dm_mdl_hotfood_restaurant_regular_duplicate_removal;
-- create table dm.dm_mdl_hotfood_restaurant_regular_duplicate_removal as
-- select t1.name, t2.single_seg
-- from (select name, max(priority) as max_priority from temp.temp_restaurant_seg_name_mapping_5 group by name) t1
-- join (select name, single_seg, priority from temp.temp_restaurant_seg_name_mapping_5) t2
-- on t1.name=t2.name and t1.max_priority=t2.priority;