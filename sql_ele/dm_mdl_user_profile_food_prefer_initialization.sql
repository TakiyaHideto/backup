#***************************************************************************************************
# ** 文件名称： dm_mdl_user_profile_food_prefer_initialization.sql
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-22
#***************************************************************************************************

set mapred.max.split.size=2000000000;
drop table temp.temp_sample_food_order_normal_30001231;
create table temp.temp_sample_food_order_normal_30001231 as 
select t1.order_id, t1.restaurant_id, t1.entity_id, t3.food_name, t2.user_id,  t2.created_at order_date, 
t3.normalize_food_name normal_food_name, case when t3.price>100 then 100 else t3.price end food_price 
from 
(select * from dw.dw_trd_order_item 
where dt>='2016-01-01' and entity_category_id=1 and (parent_entity_id is null or parent_entity_id=0)) t1 
join 
(select * from dw.dw_trd_order_wide_day where dt>='2016-01-01' and order_status=1 and user_id<>'886') t2 
on (t1.order_id=t2.id) 
join 
(select * from dm.dm_mdl_food_name_normalize_day where dt='3000-12-31') t3 on (t1.entity_id=t3.food_id);

drop table temp.temp_sample_food_order_normal_aggre_30001231;
create table temp.temp_sample_food_order_normal_aggre_30001231 as 
SELECT order_id, restaurant_id, entity_id, food_name, user_id, order_date, normal_food_name, food_price
FROM temp.temp_sample_food_order_normal_30001231
GROUP BY order_id, restaurant_id, entity_id, food_name, user_id, order_date, normal_food_name, food_price;

DROP TABLE temp.temp_mdl_user_food_sample_day_accumulation;
CREATE TABLE temp.temp_mdl_user_food_sample_day_accumulation AS
select t1.food_name, t1.user_id, t1.order_num, t1.total_price, t1.last_order_time, t2.p user_month_order_num from 
(
select normal_food_name food_name, user_id, count(distinct order_id) order_num, 
sum(food_price) total_price, max(order_date) last_order_time 
from temp.temp_sample_food_order_normal_aggre_30001231 where length(normal_food_name)>1 
group by normal_food_name, user_id
) t1 join 
(
select user_id, count(distinct order_id) p from temp.temp_sample_food_order_normal_aggre_30001231 
where length(normal_food_name)>1 
group by user_id
) t2 on (t1.user_id=t2.user_id);


--计算每个user每个food的分数
DROP TABLE temp.temp_rec_user_food_score_accumulation;
CREATE TABLE temp.temp_rec_user_food_score_accumulation AS
SELECT user_id, food_name, 
(order_num/user_month_order_num)*bound_data(total_price,70,1000)/(datediff('${day}',last_order_time)+1) as score
FROM temp.temp_mdl_user_food_sample_day_accumulation;

DROP TABLE temp.temp_rec_user_food_score_normalized;
CREATE TABLE temp.temp_rec_user_food_score_normalized AS
SELECT t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.food_name,'\":','\"',t.score_normalized,'\"'))), '}') food_info
FROM(
SELECT user_id, food_name, round(1/(1+exp(-score)),2) as score_normalized
FROM temp.temp_rec_user_food_score_accumulation) t
GROUP BY t.user_id; 

drop table temp.temp_ups_user_food_prefer_partition;
create table temp.temp_ups_user_food_prefer_partition as 
select t.user_id, t.attr_value
from(
select t1.user_id, t1.food_info as attr_value
from(select user_id, food_info from temp.temp_rec_user_food_score_normalized) t1) t
group by t.user_id, t.attr_value;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_food')
select user_id, 'rec' as top_category,'food_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition;



-- flavour
DROP TABLE temp.temp_food_tag_classification_flavor_mapping;
CREATE TABLE temp.temp_food_tag_classification_flavor_mapping AS
SELECT t4.key, t4.priority, t3.tag, t3.flavor, t3.method
FROM(
SELECT t2.key, t2.priority, t1.tag, t1.flavor, t1.method
FROM (
SELECT *
FROM dim.dim_mdl_food_tag_classification
WHERE part='class3'
) t1
JOIN (
SELECT * 
FROM dim.dim_mdl_food_tag_classification
WHERE part='class2'
) t2
ON t1.key=t2.tag
) t3
JOIN (
SELECT *
FROM dim.dim_mdl_food_tag_classification
WHERE part='class1'
) t4
ON t3.key=t4.tag;

DROP TABLE temp.temp_mdl_user_food_flavor_accumulation;
CREATE TABLE temp.temp_mdl_user_food_flavor_accumulation AS
SELECT t2.flavor, t1.user_id, sum(order_num) AS order_num, sum(total_price) AS total_price, 
max(last_order_time) AS last_order_time, sum(user_month_order_num) AS user_month_order_num, '2016-08-22' as record_day
FROM (SELECT * FROM temp.temp_mdl_user_food_sample_day_accumulation) t1
JOIN (SELECT * FROM temp.temp_food_tag_classification_flavor_mapping) t2
ON t2.key=t1.food_name
WHERE t2.flavor!='无口味' and t2.flavor!='暂无'
GROUP BY t1.user_id, t2.flavor;

drop table temp.temp_rec_user_flavor_score;
create table temp.temp_rec_user_flavor_score as
select user_id, flavor, 
(order_num/user_month_order_num)*bound_data(total_price,2,100)/datediff(record_day,last_order_time) as score
from temp.temp_mdl_user_food_flavor_accumulation;

drop table temp.temp_rec_user_flavor_score_normalized;
create table temp.temp_rec_user_flavor_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.flavor,'\":','\"',t.score_normalized,'\"'))), '}') flavor_info
from(
select user_id, flavor, round(1/(1+exp(-score)),2) as score_normalized
from temp.temp_rec_user_flavor_score) t
group by t.user_id; 

drop table temp.temp_ups_user_flavor_prefer_partition;
create table temp.temp_ups_user_flavor_prefer_partition as 
select t.user_id, t.attr_value
from(
select t2.user_id, t2.flavor_info as attr_value 
from(select user_id, flavor_info from temp.temp_rec_user_flavor_score_normalized) t2) t
group by t.user_id, t.attr_value;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_flavor')
select user_id, 'rec' as top_category,'flavor_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_flavor_prefer_partition;




-- style
DROP TABLE temp.temp_food_tag_classification_mapping;
CREATE TABLE temp.temp_food_tag_classification_mapping AS
SELECT t4.key, t4.priority, t3.tag, t3.flavor, t3.method
FROM(
SELECT t2.key, t2.priority, t1.tag, t1.flavor, t1.method
FROM (
SELECT *
FROM dim.dim_mdl_food_tag_classification
WHERE part='class3'
) t1
JOIN (
SELECT * 
FROM dim.dim_mdl_food_tag_classification
WHERE part='class2'
) t2
ON t1.key=t2.tag
) t3
JOIN (
SELECT *
FROM dim.dim_mdl_food_tag_classification
WHERE part='class1'
) t4
ON t3.key=t4.tag;

DROP TABLE temp.temp_food_tag_classification_style_mapping;
CREATE TABLE temp.temp_food_tag_classification_style_mapping AS
SELECT t2.key, t1.single_tag as style
FROM(
SELECT key, single_tag
FROM (SELECT key, tag FROM dim.dim_mdl_food_tag_classification WHERE part='food_tag_class4_func') t
LATERAL VIEW EXPLODE(split(tag,'#')) tmp AS single_tag
WHERE single_tag is not null and single_tag!='') t1
JOIN (
SELECT key, single_tag
FROM(SELECT key, concat_ws('#', tag, flavor, method) as food_tag FROM temp.temp_food_tag_classification_mapping) t
LATERAL VIEW EXPLODE(split(food_tag,'#')) tmp AS single_tag
) t2
ON t2.single_tag=t1.key;

DROP TABLE temp.temp_mdl_user_food_style_day;
CREATE TABLE temp.temp_mdl_user_food_style_day AS
SELECT t2.style, t1.user_id, sum(order_num) AS order_num, sum(total_price) AS total_price, 
max(last_order_time) AS last_order_time, sum(user_month_order_num) AS user_month_order_num, '2016-08-22' as record_day
FROM (SELECT * FROM temp.temp_mdl_user_food_sample_day_accumulation) t1
JOIN (SELECT * FROM temp.temp_food_tag_classification_style_mapping) t2
ON t2.key=t1.food_name
WHERE t2.style!='无口味' and t2.style!='暂无'
GROUP BY t1.user_id, t2.style;

drop table temp.temp_rec_user_style_score;
create table temp.temp_rec_user_style_score as
select user_id, style, 
(order_num/user_month_order_num)*bound_data(total_price,10,200)/(datediff(record_day,last_order_time)+1) as score
from temp.temp_mdl_user_food_style_day;

drop table temp.temp_rec_user_style_score_normalized;
create table temp.temp_rec_user_style_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.style,'\":','\"',t.score_normalized,'\"'))), '}') style_info
from(
select user_id, style, round(1/(1+exp(-score)),2) as score_normalized
from temp.temp_rec_user_style_score) t
group by t.user_id; 

drop table temp.temp_ups_user_style_prefer_partition;
create table temp.temp_ups_user_style_prefer_partition as 
select t.user_id, t.attr_value
from(
select t2.user_id as user_id, t2.style_info as attr_value 
from(select user_id, style_info from temp.temp_rec_user_style_score_normalized) t2) t
group by t.user_id, t.attr_value;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_style')
select user_id, 'rec' as top_category,'style_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_style_prefer_partition;


-- category
DROP TABLE temp.temp_mdl_user_food_sample_cate_day;
CREATE TABLE temp.temp_mdl_user_food_sample_cate_day AS
SELECT t1.tag as cate, t2.user_id, sum(t2.order_num) as order_num, sum(t2.total_price) as total_price,
max(t2.last_order_time) as last_order_time, sum(t2.user_month_order_num) as user_month_order_num
FROM(
SELECT key, tag
FROM dim.dim_mdl_food_tag_classification 
WHERE part='class1_3') t1
JOIN(
SELECT *
FROM temp.temp_mdl_user_food_sample_day_accumulation
) t2
ON t1.key=t2.food_name
GROUP BY t1.tag, t2.user_id;

DROP TABLE temp.temp_rec_user_cate_score;
CREATE TABLE temp.temp_rec_user_cate_score as
select user_id, cate, 
(order_num/user_month_order_num)*bound_data(total_price,6,200)/datediff('2016-08-22',last_order_time) as score
from temp.temp_mdl_user_food_sample_cate_day;

drop table temp.temp_rec_user_cate_score_normalized;
create table temp.temp_rec_user_cate_score_normalized as
select t.user_id, concat('{', concat_ws(',',collect_set(concat('\"',t.cate,'\":','\"',t.score_normalized,'\"'))), '}') food_info
from(
select user_id, cate, round(1/(1+exp(-score)),2) as score_normalized
from temp.temp_rec_user_cate_score) t
group by t.user_id; 

drop table temp.temp_ups_user_food_prefer_partition;
create table temp.temp_ups_user_food_prefer_partition as 
select t.user_id, t.attr_value
from(select t2.user_id, t2.food_info as attr_value 
from(select user_id, food_info from temp.temp_rec_user_cate_score_normalized) t2) t
group by t.user_id, t.attr_value;


insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_category')
select user_id, 'rec' as top_category,'category_prefer' as attr_key, attr_value, '1' as is_json, from_unixtime(unix_timestamp()) as update_time
from temp.temp_ups_user_food_prefer_partition;



