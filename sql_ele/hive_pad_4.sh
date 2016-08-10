output_file1=/home/jiahao.dong/hive_output/temp_result1
output_file2=/home/jiahao.dong/hive_output/temp_result2
output_file3=/home/jiahao.dong/hive_output/temp_result3

# day_index=3
# dt=`date -d -${day_index}day +%Y%m%d`
# day=`date -d -${day_index}day +%Y-%m-%d`

# echo dt
# echo $day

# rm $output_file2
# touch $output_file2
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
# 			WHERE dt='${day_2}' AND datediff(dt,last_order_time)<30 GROUP BY user_id HAVING rec_num<10) t4
# 		ON t3.user_id=t4.user_id) t2 
# 		ON (t1.user_id=t2.user_id) 
# 		GROUP BY t2.rec_method) t
# 		GROUP BY t.rec_method, t.user_uniq_cnt, t.entry_freq_cnt, t.order_uniq_cnt, t.order_freq_cnt, t.order_user_cnt
# 	;" >> $output_file2
# 	echo "\n" >> $output_file2
# done


# for (( i = 1; i < 10; i++ )); do
# 	day_index_1=$i
# 	day_index_2=`expr $i + 1`
# 	day_1=`date -d -${day_index_1}day +%Y-%m-%d`
# 	day_2=`date -d -${day_index_2}day +%Y-%m-%d`
# 	echo $day_1
# 	echo $day_2
# 	hive -i /home/jiahao.dong/backup/init.sql -e"
# 		INSERT OVERWRITE TABLE temp.temp_hotfood_rec_method_evaluation_table_1 partition(dt='${day_1}')
# 		SELECT t6.user_id, collect_set(t6.single_food_name), t7.food_name, t6.rec_method
# 		FROM(
# 		SELECT t4.user_id, t4.single_food_name, t5.rec_method,
# 		row_number() over(partition by t4.user_id, t4.is_fresh order by t4.score desc) as rno
# 		FROM (
# 		SELECT t1.user_id, t1.single_food_name, 
# 		parse_json_object(parse_json_object(t2.attr_value,t1.single_food_name),'score') AS score,
# 		parse_json_object(parse_json_object(t2.attr_value,t1.single_food_name),'is_fresh') AS is_fresh
# 		FROM (
# 		SELECT user_id, single_food_name FROM (select * from rec.rec_hotfood_user_info where dt='${day_2}' and  model='food_prefer') t3
# 		lateral view explode(get_json_keys(attr_value)) myTable AS single_food_name) t1
# 		JOIN (SELECT * FROM rec.rec_hotfood_user_info WHERE dt='${day_2}' and  model='food_prefer') t2
# 		ON t1.user_id=t2.user_id
# 		) t4
# 		JOIN (SELECT user_id, rec_method FROM dm.dm_mdl_hotfood_rec_user_model_day WHERE dt='${day_2}' and model='hotfood') t5
# 		ON t4.user_id=t5.user_id
# 		) t6
# 		JOIN (SELECT user_id, food_name FROM dm.dm_mdl_user_food_sample_day WHERE dt='${day_1}') t7
# 		ON t6.user_id=t7.user_id
# 		WHERE (t6.rno<4 and t6.rec_method not like '%click_based%') or (t6.rno<7 and t6.rec_method like '%click_based%')
# 		GROUP BY t6.user_id, t6.rec_method, t7.food_name;
# 	"
# done

