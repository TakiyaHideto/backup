-- drop table temp.temp_single_distinct_restraurant_name;
-- create table temp.temp_single_distinct_restraurant_name (rest_name string, split_name string) partitioned by (step string);

-- insert overwrite table temp.temp_single_distinct_restraurant_name partition(step='ori') 
-- select name, "ori_name"
-- from dw.dw_prd_restaurant
-- where dt='2016-07-12'
-- group by name;


-- insert overwrite table temp.temp_single_distinct_restraurant_name partition(step='seg')
-- select rest_name, single_name 
-- from (select rest_name, segmenter(rest_name,true) as name_arr1, non_single_segment(rest_name,true) as name_arr2 from temp.temp_single_distinct_restraurant_name where step='ori') t
-- lateral view explode(t.name_arr2) myTable as single_name;


-- set hive.mapred.mode=nonstrict;
-- insert overwrite table temp.temp_connect_restaurant_${dt} partition(step='p_name_mapping')
-- select t1.id restaurant_id, max(t2.category),1
-- from (select * from dw.dw_prd_restaurant where dt='2016-07-12') t1 join (select pattern, category from dim.dim_mdl_common_restaurant_name) t2
-- where t1.name like t2.pattern 
-- group by t1.id;

-- select t2.rec_method, 
-- sum(case when t1.entry_click_pv>0 then 1 else 0 end) uv, 
-- sum(case when t1.order_num>0 then 1 else 0 end) order_uv, 
-- count(distinct case when t1.order_num>0 then t1.user_id else 0 end) order_user, 
-- sum(order_num) order_num 
-- from  (select * from dw.dw_log_hotfood_user_day_inc where dt='2016-07-13') t1
-- join (select * from dm.dm_mdl_hotfood_rec_user_model_day where dt='2016-07-11') t2 
-- on (t1.user_id=t2.user_id) 
-- group by t2.rec_method;

-- select sum(case when entry_click_pv>0 then 1 else 0 end) uv, 
-- sum(case when order_num>0 then 1 else 0 end) order_uv, sum(order_num) order_num 
-- from dw.dw_log_hotfood_user_day_inc where 
-- dt='2016-07-13' and user_id is null

-- select user_id, sum(entry_click_pv), sum(food_click_pv), sum(order_num) from dw.dw_log_hotfood_user_day_inc where user_id is null and dt='2016-07-13' group by user_id;


-- select t2.rec_method, t1.dt, sum(case when t1.entry_click_pv>0 then 1 else 0 end) uv, 
-- sum(case when t1.order_num>0 then 1 else 0 end) order_uv, 
-- count(distinct case when t1.order_num>0 then t1.user_id else 0 end) order_user, 
-- sum(order_num) order_num from  
-- (select * from dw.dw_log_hotfood_user_day_inc where dt>'2016-07-14') t1
-- join (select * from dm.dm_mdl_hotfood_rec_user_model_day where dt='2016-07-14') t2 
-- on (t1.user_id=t2.user_id) 
-- where pmod(t1.user_id,17)!=2
-- group by t2.rec_method, t1.dt;




-- create table temp.temp_ori_food_seg as 
-- select normal_food_name, food_pattern
-- from (select normal_food_name from dim.dim_mdl_common_food_name where part='base') t
-- lateral view explode(non_single_segment(normal_food_name,true)) myTable as food_pattern;

-- select normal_food_name, collect_set(food_attr)
-- from (select * from temp.temp_food_attr where part='base')t1
-- join temp_ori_food_seg t2
-- on t1.food_attr=t2.food_pattern
-- group by normal_food_name
-- limit 100;


-- select normal_food_name, non_single_segment(normal_food_name,true)
-- from dim.dim_mdl_common_food_name
-- where part='base' and (array_contains(non_single_segment(normal_food_name,true),'锅兔') or array_contains(segmenter(normal_food_name,true),'锅兔'));


-- create EXTERNAL table if not EXISTS temp.temp_food_attr(food_attr string) partitioned by (part string)




select seg_food_name, count(distinct name) as num
from(
select name, segmenter(name,true,'food_name.dic','stop.dic') as food_set
from temp.temp_prd_distinct_food_name
) t
lateral view explode(food_set) temp_table as seg_food_name
group by seg_food_name
sort by num desc
limit 300;




# #将关联规则中的食品串展开
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_hotfood_assoc_user_food_prefix_explode_${dt};
# 	create table temp.temp_hotfood_assoc_user_food_prefix_explode_${dt} as 
# 	select single_food_name, prefix_food_list, size(split(prefix_food_list, '#')) size 
# 	from (select distinct prefix_food_list from dm.dm_mdl_food_assoc_rule_day where dt='3000-12-31') t
# 	lateral view explode(split(prefix_food_list, '#')) myTable as single_food_name;
# "




DROP TABLE temp.temp_food_pattern_table_copy;
CREATE TABLE temp.temp_food_pattern_table_copy (
food_name_pattern STRING, 
normal_food_name STRING,
priority DOUBLE,
last_update_time STRING
) PARTITIONED BY (part STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/temp_food_pattern_table_copy.txt' OVERWRITE INTO TABLE  temp.temp_food_pattern_table_copy PARTITION (part='base');



