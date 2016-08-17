set mapred.reduce.tasks=50;
drop table temp.temp_user_food_score_info_${dt};
create table temp.temp_user_food_score_info_${dt} as 
select user_id, food_name, min(time_gap) as elapse_hour, max(buy_flag) as is_buy
from(	select user_id, normal_food_name food_name, min(datediff('${day}', dt)*24-hour+24) time_gap, '0' as buy_flag
		from dw.dw_log_user_food_click_day_inc 
		where dt>=get_date('${day}', -6) and dt<='${day}' and length(normal_food_name)>0 and user_id>0 and user_id<>886
		group by user_id, normal_food_name
		union all 
		select user_id, text_mapping(keyword, 'food_name.txt', 1, 0) food_name, min(datediff('${day}', dt)*24-hour+24) time_gap, '0' as buy_flag
		from dw.dw_log_user_search_day_inc
		where dt>=get_date('${day}', -6) and dt<='${day}' and length(text_mapping(keyword, 'food_name.txt', 1, 0))>0 and user_id>0 and user_id<>886
		group by user_id, text_mapping(keyword, 'food_name.txt', 1, 0)
		union all
		select user_id, food_name, min(datediff('${day}', last_order_time)*24) as time_gap, '1' as buy_flag
		from dm.dm_mdl_user_food_sample_day where dt='${day}' 
		group by user_id, food_name) t
group by user_id, food_name;


set mapred.reduce.tasks=50;
drop table temp.temp_user_food_buy_click_search_score_${dt};
create table temp.temp_user_food_buy_click_search_score_${dt} as 
select t1.user_id, t1.food_name, t1.is_buy, t2.rec_method, min(t1.elapse_hour) as score
from (
	select user_id, food_name, elapse_hour, is_buy
	from temp.temp_user_food_score_info_${dt}) t1
join (
	select user_id, rec_method
	from dm.dm_mdl_hotfood_rec_user_model_day 
	where dt='${day}' and model='hotfood' and rec_method in ('click_based_only_buy','click_based_buy_click','click_based_only_click','click_based_none')) t2
on t1.user_id=t2.user_id
group by t1.user_id, t1.food_name, t2.rec_method, t1.is_buy
having rec_method<>'click_based_only_buy' or is_buy='1';


drop table temp.temp_user_food_buy_click_search_score_final_${dt};
create table temp.temp_user_food_buy_click_search_score_final_${dt} as  
select user_id, food_name as rec_food_name, score , '1' as is_fresh
from (select user_id, food_name, score, row_number() over(partition by user_id order by score desc) as rno 
		from temp.temp_user_food_buy_click_search_score_${dt}) t
where t.rno<20;


insert overwrite table rec.rec_hotfood_user_food_rec partition(dt='${day}', model='food_prefer_click_based') 
select user_id,'food_prefer', concat('{', concat_ws(',', collect_set(
concat('\"', rec_food_name,'\":\{\"score\":', cast(round(score,2) as string), ', \"is_fresh\":', is_fresh, '\}'))), '}') info,
from_unixtime(unix_timestamp()) as time
from temp.temp_user_food_buy_click_search_score_final_${dt}
group by user_id;



