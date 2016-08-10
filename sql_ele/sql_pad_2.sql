SELECT t.rec_method, 
t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt,
t.order_user_cnt/t.user_uniq_cnt,
t.order_uniq_cnt/t.user_uniq_cnt,
t.order_freq_cnt/t.entry_freq_cnt 
FROM (
SELECT t2.rec_method, 
sum(case when t1.entry_click_pv>0 then 1 else 0 end) AS user_uniq_cnt, 
sum(t1.entry_click_pv) AS entry_freq_cnt,
sum(case when t1.order_num>0 then 1 else 0 end) AS order_uniq_cnt, 
sum(order_num) AS order_freq_cnt,
count(distinct case when t1.order_num>0 then t1.user_id else 0 end) AS order_user_cnt 
FROM (SELECT * FROM dw.dw_log_hotfood_user_day_inc WHERE dt='2016-08-01') t1
JOIN (SELECT * FROM dm.dm_mdl_hotfood_rec_user_model_day WHERE dt='2016-07-29') t2 
ON (t1.user_id=t2.user_id) 
GROUP BY t2.rec_method) t
GROUP BY t.rec_method, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt;


SELECT t.rec_method, 
t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt,
t.order_user_cnt/t.user_uniq_cnt,
t.order_uniq_cnt/t.user_uniq_cnt,
t.order_freq_cnt/t.entry_freq_cnt 
FROM (
SELECT t2.rec_method, 
sum(case when t1.entry_click_pv>0 then 1 else 0 end) AS user_uniq_cnt, 
sum(t1.entry_click_pv) AS entry_freq_cnt,
sum(case when t1.order_num>0 then 1 else 0 end) AS order_uniq_cnt, 
sum(order_num) AS order_freq_cnt,
count(distinct case when t1.order_num>0 then t1.user_id else 0 end) AS order_user_cnt 
FROM (SELECT * FROM dw.dw_log_hotfood_user_day_inc WHERE dt='2016-07-26') t1
JOIN (SELECT t4.user_id, t3.rec_method 
FROM (SELECT * FROM dm.dm_mdl_hotfood_rec_user_model_day WHERE dt='2016-07-24') t3
JOIN (SELECT user_id, max(size(split(attr_value,','))) as rec_num FROM rec.rec_hotfood_user_info WHERE model='food_prefer' AND dt='2016-07-24' GROUP BY user_id HAVING rec_num>19) t4
ON t3.user_id=t4.user_id) t2 
ON (t1.user_id=t2.user_id) 
GROUP BY t2.rec_method) t
GROUP BY t.rec_method, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt;



SELECT t.rec_method, 
t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt,
t.order_user_cnt/t.user_uniq_cnt,
t.order_uniq_cnt/t.user_uniq_cnt,
t.order_freq_cnt/t.entry_freq_cnt 
FROM (
SELECT t2.rec_method, 
sum(case when t1.entry_click_pv>0 then 1 else 0 end) AS user_uniq_cnt, 
sum(t1.entry_click_pv) AS entry_freq_cnt,
sum(case when t1.order_num>0 then 1 else 0 end) AS order_uniq_cnt, 
sum(order_num) AS order_freq_cnt,
count(distinct case when t1.order_num>0 then t1.user_id else 0 end) AS order_user_cnt 
FROM (SELECT * FROM dw.dw_log_hotfood_user_day_inc WHERE dt='2016-08-01') t1
JOIN (SELECT t4.user_id, t3.rec_method 
FROM (SELECT * FROM dm.dm_mdl_hotfood_rec_user_model_day WHERE dt='2016-07-30') t3
JOIN (SELECT user_id, count(distinct food_name) as rec_num FROM dm.dm_mdl_user_food_sample_day WHERE dt='2016-07-30' GROUP BY user_id HAVING rec_num>10) t4
ON t3.user_id=t4.user_id) t2 
ON (t1.user_id=t2.user_id) 
GROUP BY t2.rec_method) t
GROUP BY t.rec_method, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt;



SELECT  t.usr_grp, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt,
t.order_user_cnt/t.user_uniq_cnt,
t.order_uniq_cnt/t.user_uniq_cnt,
t.order_freq_cnt/t.entry_freq_cnt 
FROM (
SELECT pmod(t1.user_id,3) as usr_grp,
sum(case when t1.entry_click_pv>0 then 1 else 0 end) AS user_uniq_cnt, 
sum(t1.entry_click_pv) AS entry_freq_cnt,
sum(case when t1.order_num>0 then 1 else 0 end) AS order_uniq_cnt, 
sum(order_num) AS order_freq_cnt,
count(distinct case when t1.order_num>0 then t1.user_id else 0 end) AS order_user_cnt 
FROM (SELECT * FROM dw.dw_log_hotfood_user_day_inc WHERE dt='2016-07-29') t1
JOIN (SELECT t3.user_id, max(buy_flag) as is_buy
FROM(
select user_id, '0' as buy_flag from dw.dw_log_user_food_click_day_inc 
where dt>=get_date('2016-07-27', -6) and dt<='2016-07-27' and length(normal_food_name)>0 and user_id>0 and user_id<>886
union all 
select user_id, '0' as buy_flag from dw.dw_log_user_search_day_inc
where dt>=get_date('2016-07-27', -6) and dt<='2016-07-27' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
union all
select user_id, '1' as buy_flag from dm.dm_mdl_user_food_sample_day where dt='2016-07-27' and user_month_order_num<60
union all 
select user_id, '2' as buy_flag from dm.dm_mdl_user_food_sample_day where dt='2016-07-27' and user_month_order_num>=60
) t3
GROUP BY t3.user_id
HAVING is_buy=0) t2 
ON (t1.user_id=t2.user_id) 
GROUP BY pmod(t1.user_id,3)) t
GROUP BY t.usr_grp, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt;



select t.user_id, t.food_name, t.sum_num
from(
select user_id, food_name, sum(order_num) as sum_num
from dm.dm_mdl_user_food_sample_day where dt='2016-08-01' and datediff(dt,last_order_time)>10 
group by user_id,food_name having sum_num>5 
order by user_id, sum_num desc
limit 100
) t;







-- 正负样本，多于1次购买的菜品为正，否则为负
SELECT user_id, food_name, case when t.food_freq>1 then 1 else 0 end AS is_like
FROM( 
SELECT user_id, food_name, count(distinct order_num) as food_freq
FROM dm.dm_mdl_user_food_sample_day
WHERE dt>'2016-06-26'
GROUP BY user_id, food_name) t
limit 200;


-- 正负样本，点击搜索的菜品为正，否则为负
select user_id, food_name, avg(click_flag) as is_click
from(
select user_id, normal_food_name food_name, '1' as click_flag
from dw.dw_log_user_food_click_day_inc 
where dt>=get_date('2016-07-26', -6) and dt<='2016-07-26' and length(normal_food_name)>0 and user_id>0 and user_id<>886
group by user_id, normal_food_name
union all 
select user_id, text_mapping(keyword, 'food_name.txt', 1, 0) food_name, '0' as click_flag
from dw.dw_log_user_search_day_inc
where dt>=get_date('2016-07-26', -6) and dt<='2016-07-26' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
group by user_id, text_mapping(keyword, 'food_name.txt', 1, 0)
) t
group by user_id, food_name
limit 100;




DROP TABLE temp.temp_restaurant_regular_name_mapping_temp;
CREATE TABLE temp.temp_restaurant_regular_name_mapping_temp(
name STRING,
regular_name STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ;
LOAD DATA LOCAL inpath '/home/jiahao.dong/file_data/restaurant_regular_name' OVERWRITE INTO TABLE  temp.temp_restaurant_regular_name_mapping_temp;

DROP TABLE temp.temp_restaurant_regular_name_mapping; 
CREATE TABLE temp.temp_restaurant_regular_name_mapping AS
SELECT concat_ws('','%',name,'%') AS pattern, regular_name AS category, length(name) AS priority
FROM temp.temp_restaurant_regular_name_mapping_temp;




insert overwrite table temp.temp_connect_restaurant_20160730 partition(step='p_original_name')
select id restaurant_id, name, 0
from dw.dw_prd_restaurant
where dt='2016-07-31';


CREATE TABLE temp.temp_hotfood_rec_method_evaluation_table_1
(
user_id bigint,
food_set array<string>,
food_name string,
rec_method string
) partitioned by (dt string);

DROP TABLE temp.temp_hotfood_rec_method_evaluation_table_1;
CREATE TABLE temp.temp_hotfood_rec_method_evaluation_table_1 AS
SELECT t6.user_id, collect_set(t6.single_food_name), t7.food_name, t6.rec_method
FROM(
SELECT t4.user_id, t4.single_food_name, t5.rec_method,
row_number() over(partition by t4.user_id, t4.is_fresh order by t4.score desc) as rno
FROM (
SELECT t1.user_id, t1.single_food_name, 
parse_json_object(parse_json_object(t2.attr_value,t1.single_food_name),'score') AS score,
parse_json_object(parse_json_object(t2.attr_value,t1.single_food_name),'is_fresh') AS is_fresh
FROM (
SELECT user_id, single_food_name FROM (select * from rec.rec_hotfood_user_info where dt='2016-07-30' and  model='food_prefer') t3
lateral view explode(get_json_keys(attr_value)) myTable AS single_food_name) t1
JOIN (SELECT * FROM rec.rec_hotfood_user_info WHERE dt='2016-07-30' and  model='food_prefer') t2
ON t1.user_id=t2.user_id
) t4
JOIN (SELECT user_id, rec_method FROM dm.dm_mdl_hotfood_rec_user_model_day WHERE dt='2016-07-30' and model='hotfood') t5
ON t4.user_id=t5.user_id
) t6
JOIN (SELECT user_id, food_name FROM dm.dm_mdl_user_food_sample_day WHERE dt='2016-07-31') t7
ON t6.user_id=t7.user_id
WHERE (t6.rno<4 and t6.rec_method not like '%click_based%') or (t6.rno<7 and t6.rec_method like '%click_based%')
GROUP BY t6.user_id, t6.rec_method, t7.food_name;




DROP TABLE temp.temp_hotfood_rec_method_evaluation_table_2;
CREATE TABLE temp.temp_hotfood_rec_method_evaluation_table_2 AS
SELECT rec_method, case when array_contains(food_set,food_name) then 1 else 0 end AS is_hit
FROM temp.temp_hotfood_rec_method_evaluation_table_1
WHERE dt='2016-08-02';
select rec_method, sum(is_hit)/count(*) from temp_hotfood_rec_method_evaluation_table_2 group by rec_method;




create external table dw.dw_log_hotfood_exposure_day_inc 
(
request_id string,
link_log_id string,
request_time string,
user_id bigint,
eleme_device_id string,
algo_version string,
rank_type string,
eleme_city_id bigint,
is_new_user int,
geohash string,
restaurant_id bigint,
restaurant_index int,
food_id bigint,
food_index int
) partitioned by (dt string) 
location '/data/external_table/dw/dw_log_hotfood_exposure_day_inc';


select * from dm.dm_mdl_food_name_normalize_day where dt='2016-08-01' and food_id='972954';



SELECT user_id, rec_food_name
FROM (SELECT user_id, rec_food_name FROM (select * from rec.rec_hotfood_user_info where dt='${day}' and model='food_prefer') t
lateral view explode(get_json_keys(attr_value)) tmpTbl AS rec_food_name


select * from dw.dw_prd_food where dt='2016-08-01' and name!='未知' limit 10;



SELECT user_id, rec_food_name 
FROM (select * from rec.rec_hotfood_user_info where dt='2016-08-01' and model='food_prefer') t
lateral view explode(get_json_keys(attr_value)) tmpTbl AS rec_food_name
limit 100;

SELECT sum(is_rec)/count(is_rec) from temp_hotfood_impression_feedback_info_day_inc_merge_3;



select t2.rec_method, sum(t1.is_rec)/count(t1.is_rec), sum(is_click)/count(is_click), sum(is_buy)/count(is_buy)
from temp_hotfood_impression_feedback_info_day_inc_merge_3 t1
join (select user_id, rec_method from dm.dm_mdl_hotfood_rec_user_model_day where dt='2016-08-03' and model='hotfood') t2
on t1.user_id=t2.user_id
group by t2.rec_method;


DROP TABLE temp.temp_restaurant_seg_name_mapping_4;
CREATE TABLE dm.dm_mdl_hotfood_restaurant_name_origin_segment_mapping AS
SELECT *
FROM temp.temp_restaurant_seg_name_mapping_4


SELECT sum(case when size(array_operator(t.set1,t.set2,2))>0 then 1 else 0 end), count(*)
FROM(
SELECT t1.user_id, set1, collect_set(case when t2.rno<21 then t2.food_name end) AS set2
FROM
(SELECT user_id, collect_set(case when datediff('2016-08-04',last_order_time)=1 then food_name end) AS set1 
FROM dm.dm_mdl_user_food_sample_day WHERE dt='2016-08-04'
GROUP BY user_id) t1
JOIN 
(SELECT user_id, food_name, row_number() over(partition by user_id order by order_num desc) as rno
FROM dm.dm_mdl_user_food_sample_day WHERE dt='2016-08-04' and datediff('2016-08-04',last_order_time)>=1) t2
ON t1.user_id=t2.user_id
GROUP BY t1.user_id, set1
) t
;

select * from dm.dm_mdl_hotfood_restaurant_distinct_day where dt='2016-08-04' and distinct_flag='和合谷' limit 100;



-- ############################################################################################################################


SELECT key, collect_set(case when tag not like '%无%' then tag end)
FROM(
SELECT t5.key, t6.value
FROM(
SELECT t3.key, t4.value
FROM(
SELECT t1.value as key, t2.value
FROM (SELECT key, value FROM temp.temp_food_tag_classification WHERE part='class1') t1
JOIN (SELECT key, value FROM temp.temp_food_tag_classification WHERE part='class2' GROUP BY key, value) t2
ON t1.value=t2.key) t3
JOIN (SELECT key, value FROM temp.temp_food_tag_classification WHERE part='class3') t4
ON t3.value=t4.key) t5
JOIN (SELECT key, value FROM temp.temp_food_tag_classification WHERE part='class4') t6
ON t5.value=t6.key
) t
LATERAL VIEW EXPLODE(split(value,'#')) tmpTbl AS tag
GROUP BY key
LIMIT 200;









