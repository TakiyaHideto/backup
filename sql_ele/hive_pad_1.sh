dt=20160714
day="2016-07-14"
echo $day
 

# #将关联规则中的食品串展开
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_hotfood_assoc_user_food_prefix_explode_${dt};
	# create table temp.temp_hotfood_assoc_user_food_prefix_explode_${dt} as 
	select single_food_name, prefix_food_list, size(split(prefix_food_list, '#')) size 
	from (select distinct prefix_food_list from dm.dm_mdl_food_assoc_rule_day where dt='3000-12-31') t
	lateral view explode(split(prefix_food_list, '#')) myTable as single_food_name;
# "

# ############################################
# # 从dm.dm_mdl_user_food_sample_day中选取需要使用关联规则的用户
# hive -e"
# 	drop table temp.temp_mdl_user_food_sample_day;
# 	create table temp.temp_mdl_user_food_sample_day as
# 	select t1.food_name, t1.user_id, t1.order_num, t1.total_price, t1.last_order_time, t1.dt, t2.rec_method
# 	from (select * from dm.dm_mdl_user_food_sample_day where dt='${day}') t1 
# 		join (select user_id, dt, rec_method from dm.dm_mdl_hotfood_rec_user_model_day 
# 				where model='hotfood' and dt='${day}' and
# 				rec_method in ('full_assoc_rule_neither','full_assoc_rule_only_buy','full_assoc_rule_only_rec','full_assoc_rule_both')) t2
# 		on t1.user_id=t2.user_id and t1.dt=t2.dt;
# "

# #通过pmod选择1/10的用户，通过food为主键，关联user和assco rule中的食品
# hive  -e"
# 	drop table temp.temp_hotfood_assoc_user_assoc_rule_relation_${dt};
# 	set mapred.max.split.size=2000000;
# 	create table temp.temp_hotfood_assoc_user_assoc_rule_relation_${dt} as 
# 	select prefix_food_list, user_id, size, last_order_time
# 	from temp.temp_hotfood_assoc_user_food_prefix_explode_${dt} t1 
# 		join temp.temp_mdl_user_food_sample_day t2
# 		on t1.single_food_name=t2.food_name;
# "

# #承接上一步sql，仅将上一步数据进行group by操作
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	set mapred.max.split.size=4096000000;
# 	drop table temp.temp_hotfood_assoc_user_assoc_rule_relation_group_${dt};
# 	create table temp.temp_hotfood_assoc_user_assoc_rule_relation_group_${dt} as 
# 	select prefix_food_list, user_id, count(*) num, max(size) size, avg((31-datediff('${day}', last_order_time))) as rec_food_time
# 	from temp.temp_hotfood_assoc_user_assoc_rule_relation_${dt} 
# 	group by prefix_food_list, user_id 
# 	having num=size;
# "


#通过prefix_food将用户和rec_food相关联
hive -i /home/jiahao.dong/backup/init.sql -e"
drop table temp.temp_hotfood_assoc_user_rec_food_${dt};
set mapred.reduce.tasks=100;
create table temp.temp_hotfood_assoc_user_rec_food_${dt} as 
select t1.user_id, t2.rec_food_name, t2.support, 
	t2.common_user_num, t2.score, 
	cast(t2.rec_food_price as string) as food_price, t1.rec_food_time
from (select prefix_food_list, user_id, rec_food_time from temp.temp_hotfood_assoc_user_assoc_rule_relation_group_${dt}) t1
	join (select * from dm.dm_mdl_food_assoc_rule_day where dt='3000-12-31') t2
	on t1.prefix_food_list=t2.prefix_food_list;
"


# # 标记推荐的菜品是否是user买过的
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_hotfood_assoc_user_rec_food_relation_${dt};
# 	set mapred.reduce.tasks=100;
# 	create table temp.temp_hotfood_assoc_user_rec_food_relation_${dt} as  
# 	select t1.user_id, t1.rec_food_name, t1.support, t1.common_user_num, t1.score, t1.food_price, t1.rec_food_time,
# 			case when array_contains(t2.food_set,rec_food_name) then '0' else '1' end is_fresh, t2.rec_method
# 	from temp.temp_hotfood_assoc_user_rec_food_${dt} t1
# 		join (select user_id, collect_set(food_name) as food_set, rec_method
# 				from temp.temp_mdl_user_food_sample_day
# 				where dt='${day}' 
# 				group by user_id,rec_method) t2 
# 		on t1.user_id=t2.user_id;
# "


# #创建分数表
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt};
# 	create table temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} (
# 	user_id bigint, 
# 	rec_food_name string,
# 	final_score double,
# 	is_fresh string,
# 	rno int
# 	) partitioned by (step string);
# "

# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	set mapred.reduce.tasks=200;
# 	insert overwrite table temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} partition(step='rec_bought')
# 	select t.user_id, t.food_name, t.final_buy_score, t.is_fresh, 
# 			row_number() over(partition by t.user_id order by t.final_buy_score desc) as rno
# 	from (select user_id, food_name, '0' as is_fresh, rec_method,
# 		case when rec_method in ('full_assoc_full_both', 'full_assoc_full_only_buy') then max((31-datediff('${day}', last_order_time))*order_num) 
# 			else max(order_num) end as final_buy_score
# 		from temp.temp_mdl_user_food_sample_day 
# 		where dt='${day}'
# 		group by user_id, food_name, rec_method) t;
# "

# # 计算推荐菜品的分数
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	set mapred.reduce.tasks=200;
# 	insert overwrite table temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} partition(step='rec_rules') 
# 	select t1.user_id, t1.rec_food_name, max(t1.score) as final_score, '1', 1
# 	from (select user_id, rec_food_name, rec_method,
# 			case when rec_method in ('full_assoc_full_both', 'full_assoc_full_only_rec')then max(rec_food_time * score * bound_data(food_price,3,50)/200.0) 
# 			else max(score * bound_data(food_price,3,50)/200.0) end score
# 			from temp.temp_hotfood_assoc_user_rec_food_relation_${dt}
# 			where is_fresh='1'
# 			group by user_id, rec_food_name,rec_method) t1 
# 	group by t1.user_id, t1.rec_food_name;
# "

# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	set mapred.reduce.tasks=100;
# 	insert overwrite table temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} partition(step='rec_rules_and_bought') 
# 	select t.user_id, t.rec_food_name, t.final_score, t.is_fresh, 
# 			row_number() over(partition by t.user_id order by is_fresh asc, t.final_score desc) as rno
# 	from (
# 		select user_id, rec_food_name, final_score, is_fresh
# 		from temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} 
# 		where is_fresh='0' and step='rec_bought' and rno<15
# 		union all
# 		select user_id, rec_food_name, final_score, is_fresh
# 		from temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} 
# 		where is_fresh='1' and step='rec_rules'
# 	) t;
# "

# # 去重相同用户推荐相同菜品
# hive -i /home/jiahao.dong/backup/init.sql -e"
# 	drop table temp.temp_hotfood_assoc_user_food_final_${dt};
# 	create table temp.temp_hotfood_assoc_user_food_final_${dt} as  
# 	select user_id, rec_food_name, max(final_score) as score, is_fresh
# 	from temp.temp_hotfood_assoc_user_rec_food_cal_score_${dt} 
# 	where (step='rec_rules_and_bought') and rno<20
# 	group by user_id, rec_food_name, is_fresh;
# "

# hive -e"
# 	insert overwrite table rec.rec_hotfood_user_food_rec partition(dt='${day}', model='food_prefer_assoc_rule') 
# 	select user_id,'food_prefer', concat('{', concat_ws(',', collect_set(
# 	concat('\"', rec_food_name,'\":\{\"score\":', cast(round(score,2) as string), ', \"is_fresh\":', is_fresh, '\}'))), '}') info,
# 	from_unixtime(unix_timestamp()) as time
# 	from temp.temp_hotfood_assoc_user_food_final_${dt}
# 	group by user_id;
# "





