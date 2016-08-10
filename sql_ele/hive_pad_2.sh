dt=20160720
day="2016-07-20"
echo $day
output_file=/home/jiahao.dong/hive_output/temp_result1
echo $output_file




# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_user_food_score_info_${dt};
# 	create table temp.temp_user_food_score_info_${dt} as 
# 	select user_id, food_name, min(time_gap) as elapse_hour, max(buy_flag) as is_buy
# 	from(	select user_id, normal_food_name food_name, min(datediff('${day}', dt)*24-hour+24) time_gap, '0' as buy_flag
# 			from dw.dw_log_user_food_click_day_inc 
# 			where dt>=get_date('${day}', -6) and dt<='${day}' and length(normal_food_name)>0 and user_id>0 and user_id<>886
# 			group by user_id, normal_food_name
# 			union all 
# 			select user_id, text_mapping(keyword, 'food_name.txt', 1, 0) food_name, min(datediff('${day}', dt)*24-hour+24) time_gap, '0' as buy_flag
# 			from dw.dw_log_user_search_day_inc
# 			where dt>=get_date('${day}', -6) and dt<='${day}' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
# 			group by user_id, text_mapping(keyword, 'food_name.txt', 1, 0)
# 			union all
# 			select user_id, food_name, min(datediff('${day}', last_order_time)*24) as time_gap, '1' as buy_flag
# 			from dm.dm_mdl_user_food_sample_day where dt='${day}' 
# 			group by user_id, food_name) t
# 	group by user_id, food_name;
# "

hive -i /home/jiahao.dong/backup/init.sql -e"
	drop table temp.temp_user_food_buy_click_search_score_${dt};
	create table temp.temp_user_food_buy_click_search_score_${dt} as 
	select t1.user_id, t1.food_name, t1.is_buy, t2.rec_method, min(t1.elapse_hour) as score
	from (
		select user_id, food_name, elapse_hour, is_buy
		from temp.temp_user_food_score_info_${dt}) t1
	join (
		select user_id, rec_method
		from temp.temp_mdl_hotfood_rec_user_model_day 
		where rec_method in ('click_based_only_buy','click_based_buy_click','click_based_only_click')) t2
	on t1.user_id=t2.user_id
	group by t1.user_id, t1.food_name, t2.rec_method, t1.is_buy
	having rec_method<>'click_based_only_buy' or is_buy='1';
"


# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_user_food_buy_click_search_score_final_${dt};
# 	create table temp.temp_user_food_buy_click_search_score_final_${dt} as  
# 	select user_id, food_name, score , '1' as is_fresh
# 	from (select user_id, food_name, score, row_number() over(partition by user_id order by score desc) as rno 
# 			from temp.temp_user_food_buy_click_search_score_${dt}) t
# 	where t.rno<10;
# "

# hive -e"
# 	insert overwrite table rec.rec_hotfood_user_food_rec partition(dt='${day}', model='food_buy_click_search') 
# 	select user_id,'food_prefer', concat('{', concat_ws(',', collect_set(
# 	concat('\"', rec_food_name,'\":\{\"score\":', cast(round(score,2) as string), ', \"is_fresh\":', is_fresh, '\}'))), '}') info,
# 	from_unixtime(unix_timestamp()) as time
# 	from temp.temp_user_food_buy_click_search_score_final_${dt}
# 	group by user_id;
# "







# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_user_food_buy_click_search_score_${dt};
# 	create table temp.temp_user_food_buy_click_search_score_${dt} as 
# 	select t1.user_id, t1.food_name, 
# 		min(case when t2.rec_method='only_buy' then elapse_hour
# 			 when t2.rec_method='buy_click' then (case when buy_date is null then click_elapse_hour else datediff('${day}',buy_date)*24 end)
# 			 when t2.rec_method='only_click' then click_elapse_hour end) score
# 	from (
# 		select user_id, food_name, elapse_hour
# 		from temp.temp_user_food_score_info_${dt}) t1
# 	join (
# 		select user_id, rec_method
# 		from temp.temp_mdl_hotfood_rec_user_model_day 
# 		where rec_method in ('only_buy','buy_click','only_click')) t2
# 	on t1.user_id=t2.user_id
# 	group by t1.user_id, t1.food_name;
# "








# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_user_food_log_${dt};
# 	create table temp.temp_user_food_log_${dt} as 
# 	select * 
# 	from(
# 		select t1.user_id, t1.food_name, t1.elapse_hour click_elapse_hour, 
# 		case when ((t2.last_order_time<t1.dt and t1.food_name=t2.food_name) or t2.last_order_time is null) then 'none_time' 
# 		when (t2.last_order_time>t1.dt and t1.food_name=t2.food_name) then t2.last_order_time else 'illegal' end buy_elapse_hour
# 		from
# 		(
# 			select user_id, normal_food_name food_name, min(datediff('${day}', dt)*24-hour+24) elapse_hour, dt
# 			from dw.dw_log_user_food_click_day_inc 
# 			where dt>=get_date('${day}', -6) and dt<='${day}' and length(normal_food_name)>0 and user_id>0 and user_id<>886
# 			group by user_id, normal_food_name, dt
# 			union all 
# 			select user_id, text_mapping(keyword, 'food_name.txt', 1, 0) food_name, min(datediff('${day}', dt)*24-hour+24) elapse_hour, dt
# 			from dw.dw_log_user_search_day_inc
# 			where dt>=get_date('${day}', -6) and dt<='${day}' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
# 			group by user_id, text_mapping(keyword, 'food_name.txt', 1, 0), dt
# 		) t1 left outer join 
# 		(
# 		select * from dm.dm_mdl_user_food_sample_day where dt='${day}' 
# 		) t2 on (t1.user_id=t2.user_id) ) t3
# 	where t3.buy_elapse_hour!='illegal';
# "

# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_user_food_log_${dt};
# 	create table temp.temp_user_food_log_${dt} as 
# 	select t3.user_id, t3.food_name, t3.click_elapse_hour, t3.buy_date
# 	from(
# 		select t1.user_id, t1.food_name, t2.food_name as food_name_t2, min(t1.elapse_hour) click_elapse_hour, 
# 		max(case when t2.last_order_time is null then 'none_time' else t2.last_order_time end) buy_date
# 		from
# 		(
# 			select user_id, normal_food_name food_name, min(datediff('${day}', dt)*24-hour+24) elapse_hour, dt
# 			from dw.dw_log_user_food_click_day_inc 
# 			where dt>=get_date('${day}', -6) and dt<='${day}' and length(normal_food_name)>0 and user_id>0 and user_id<>886
# 			group by user_id, normal_food_name, dt
# 			union all 
# 			select user_id, text_mapping(keyword, 'food_name.txt', 1, 0) food_name, min(datediff('${day}', dt)*24-hour+24) elapse_hour, dt
# 			from dw.dw_log_user_search_day_inc
# 			where dt>=get_date('${day}', -6) and dt<='${day}' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
# 			group by user_id, text_mapping(keyword, 'food_name.txt', 1, 0), dt
# 		) t1 left outer join 
# 		(
# 		select * from dm.dm_mdl_user_food_sample_day where dt='${day}' 
# 		) t2 on (t1.user_id=t2.user_id) 
# 		group by t1.user_id, t1.food_name, t2.food_name) t3
# 	where t3.food_name=t3.food_name_t2 or t3.food_name_t2 is null;
# "

# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_user_food_log_${dt};
# 	create table temp.temp_user_food_log_${dt} as 
# 	select t.user_id, max(t.buy_flag) as is_buy
# 	from(
# 			select user_id, '0' as buy_flag from dw.dw_log_user_food_click_day_inc 
# 				where dt>=get_date('${day}', -6) and dt<='${day}' and length(normal_food_name)>0 and user_id>0 and user_id<>886
# 			union all 
# 			select user_id, '0' as buy_flag from dw.dw_log_user_search_day_inc
# 				where dt>=get_date('${day}', -6) and dt<='${day}' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
# 			union all
# 			select user_id, '1' as buy_flag from dm.dm_mdl_user_food_sample_day where dt='${day}'
# 		) t
# 	group by t.user_id;
# "

# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_mdl_hotfood_rec_user_model_day;
# 	create table temp.temp_mdl_hotfood_rec_user_model_day as
# 	select '${day}' as log_date, user_id, 'hotfood_food_rec' as rec_app, 
# 	max(case 
# 	when pmod(user_id,17)<6 and is_buy='1' and pmod(user_id,2)=0 then 'click_based_only_buy' 
# 	when pmod(user_id,17)<6 and is_buy='1' and pmod(user_id,2)=1 then 'click_based_buy_click'
# 	when pmod(user_id,17)<6 and is_buy='0' and pmod(user_id,2)=0 then 'click_based_only_click'
# 	when pmod(user_id,17)<6 and is_buy='0' and pmod(user_id,2)=1 then 'click_based_none'
# 	when pmod(user_id,17)<7 and is_buy='1' then 'item_based' 
# 	when pmod(user_id,17)<8 and is_buy='1' then 'none' 
# 	when pmod(user_id,17)<9 and is_buy='1' then 'assoc_rule_level1' 
# 	when pmod(user_id,17)<11 and is_buy='1' then 'full_assoc_rule_neither' 
# 	when pmod(user_id,17)<13 and is_buy='1' then 'full_assoc_rule_only_buy' 
# 	when pmod(user_id,17)<15 and is_buy='1' then 'full_assoc_rule_only_rec'
# 	when pmod(user_id,17)<17 and is_buy='1' then 'full_assoc_rule_both' end) as rec_method
# 	from temp.temp_user_food_log_${dt}
# 	group by user_id;
# "

