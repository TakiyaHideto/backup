output_file1=/home/jiahao.dong/hive_output/temp_result1
output_file2=/home/jiahao.dong/hive_output/temp_result2
output_file3=/home/jiahao.dong/hive_output/temp_result3

day_index=2
dt=`date -d -${day_index}day +%Y%m%d`
day=`date -d -${day_index}day +%Y-%m-%d`

echo $dt
echo $day

# hive -i /home/jiahao.dong/backup/init.sql -e"
# DROP TABLE temp.temp_hotfood_user_rec_food_restaurant_mapping_${dt};
# set mapred.reduce.tasks=200;
# DROP TABLE temp.temp_hotfood_user_rec_food_restaurant_mapping_${dt};
# CREATE TABLE temp.temp_hotfood_user_rec_food_restaurant_mapping_${dt} AS
# SELECT t.user_id, t.food_name, t.food_id, max(t.rec_flag) AS is_rec
# FROM( 
# 	SELECT t1.user_id, t1.rec_food_name AS food_name, t2.food_id, 1 AS rec_flag 
# 		FROM (SELECT user_id, rec_food_name 
# 			  FROM (select * from rec.rec_hotfood_user_info where dt='${day}' and model='food_prefer') t
# 			  lateral view explode(get_json_keys(attr_value)) tmpTbl AS rec_food_name) t1
# 		JOIN (SELECT user_id, food_id, food_name, normalize_food_name FROM temp.temp_hotfood_impression_food_name_mapping_${dt}) t2
# 		ON t1.rec_food_name=t2.normalize_food_name and t1.user_id=t2.user_id
# 	UNION ALL
# 	SELECT user_id, normalize_food_name AS food_name, food_id, 0 AS rec_flag FROM temp.temp_hotfood_impression_food_name_mapping_${dt}
# ) t
# GROUP BY t.user_id, t.food_name, t.food_id;
# "
 
# rm $output_file1
# touch $output_file1
# for i in {1..8}
# do
# 	day_index_1=$i
# 	day_index_2=`expr $i + 2`
# 	day_1=`date -d -${day_index_1}day +%Y-%m-%d`
# 	day_2=`date -d -${day_index_2}day +%Y-%m-%d`
# 	echo $day_1
# 	echo $day_2
# 	hive -i /home/jiahao.dong/backup/init.sql -e"
# 		SELECT t.rec_method, 
# 		t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt,
# 		t.order_user_cnt/t.user_uniq_cnt,
# 		t.order_uniq_cnt/t.user_uniq_cnt,
# 		t.order_freq_cnt/t.entry_freq_cnt 
# 		FROM (
# 		SELECT t2.rec_method, 
# 		sum(case when t1.entry_click_pv>0 then 1 else 0 end) AS user_uniq_cnt, 
# 		sum(t1.entry_click_pv) AS entry_freq_cnt,
# 		sum(case when t1.order_num>0 then 1 else 0 end) AS order_uniq_cnt, 
# 		sum(order_num) AS order_freq_cnt,
# 		count(distinct case when t1.order_num>0 then t1.user_id else 0 end) AS order_user_cnt 
# 		FROM (SELECT * FROM dw.dw_log_hotfood_user_day_inc WHERE dt='${day_1}') t1
# 		JOIN (SELECT t4.user_id, t3.rec_method 
# 		FROM (SELECT * FROM dm.dm_mdl_hotfood_rec_user_model_day WHERE dt='${day_2}') t3
# 		JOIN (SELECT user_id, count(distinct food_name) as rec_num FROM dm.dm_mdl_user_food_sample_day 
# 			WHERE dt='${day_2}' AND datediff(dt,last_order_time)<30 GROUP BY user_id HAVING rec_num>10) t4
# 		ON t3.user_id=t4.user_id) t2 
# 		ON (t1.user_id=t2.user_id) 
# 		GROUP BY t2.rec_method) t
# 		GROUP BY t.rec_method, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt
# 	;" >> $output_file1
# 	echo "\n" >> $output_file1
# done


# hive -i /home/jiahao.dong/backup/init.sql -e"
# set mapred.max.split.size=4096000000;
# select user_id, count(id) 
# from dw.dw_trd_eleme_order 
# where dt>'2016-07-25' 
# group by user_id;
# " >$output_file


hive -i /home/jiahao.dong/backup/init.sql -e"
	select * from temp_food_cat_name_tag_mapping where part='cate_tag_group' and priority=1;
" > $output_file3

# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_prd_distinct_food_name;
# 	create table temp.temp_prd_distinct_food_name as 
# 	select name 
# 	from dw.dw_prd_food
# 	where dt>'2016-07-20' and name!='未知'
# 	group by name
# "

# hive -i /home/jiahao.dong/backup/init.sql -e"
# set hive.mapred.mode=nonstrict;
# select t.restaurant_id, t.regular_name, row_number() over(partition by t.restaurant_id order by t.priority desc) 
# from 
# (select t1.id restaurant_id, t2.category as regular_name, t2.priority
# from (select * from dw.dw_prd_restaurant where dt='${day}') t1 join (select pattern, category, priority from dim.dim_mdl_common_restaurant_name) t2
# where t1.name like t2.pattern) t;
# "  > $output_file2

# hive -i /home/jiahao.dong/backup/init.sql -e"

# insert overwrite table temp.temp_connect_restaurant_${dt} partition(step='p_original_name')
# select id restaurant_id, name, 0
# from dw.dw_prd_restaurant
# where dt='${day}';
# " 

