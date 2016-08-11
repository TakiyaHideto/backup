drop table temp.temp_restaurant_for_zf_zyl;
create table temp.temp_restaurant_for_zf_zyl as 
select t1.restaurant_id, t1.restaurant_name, t1.is_premium, t1.total_work_day, 
t1.city_name, t2.is_gka, t2.avg_order, t2.avg_collections, t2.users, t2.like_rate, 
t2.negative_complaints_rate, t2.negative_restaurant_withdraw_order_rate, t2.negative_user_withdraw_order_rate, 
t2.negative_bad_rating_rate, t2.reminder_rate, t2.customer_order_price, t3.city_degree, t2.order_num,
row_number() over (partition by t1.city_name order by t2.customer_order_price) price_degree, 
row_number() over (partition by t1.city_name order by t2.avg_order) order_num_degree
from 
(
select restaurant_id, restaurant_name, city_name, is_premium, total_work_day, city_id  
from rec.rec_prd_restaurant_rank_indicator where dt='2016-08-10' and is_certification=1
) t1 
join 
(
select id, is_gka, avg_order, avg_collections, users, like_rate, negative_complaints_rate, is_exclusive,
negative_restaurant_withdraw_order_rate, negative_user_withdraw_order_rate, order_num,  subsidy_score, 
exposure_score, 
negative_bad_rating_rate, reminder_rate, customer_order_price 
from rec.rec_prd_restaurant_rank_info 
where dt='2016-08-10'
) t2 
on (t1.restaurant_id=t2.id) 
join 
dim.dim_gis_city t3 
on (t1.city_id=t3.eleme_city_id);


select * from temp.temp_restaurant_for_zf_zyl limit 10;

drop table temp.temp_restaurant_score_for_zf;
create table temp.temp_restaurant_score_for_zf as 
select restaurant_id, restaurant_name, t1.city_name,
4+is_premium*2.5+is_gka*2.5 pinpai_score,
bound_data(total_work_day/30, 1, 10) work_day_score, 
bound_data(3*log(users+1)/log(10), 0, 10) user_score,
10*bound_data((order_num+10)/(users+10), 1,2 )-10 back_user_score1, 
order_num_degree/num*10 order_score,
price_degree/num*10 price_score,
bound_data(25*log(avg_collections+1)/log(users+100), 0,5) collection_score, 
bound_data(negative_restaurant_withdraw_order_rate*40,0, 3) negative_restaurant_withdraw_score,
bound_data(negative_user_withdraw_order_rate*20, 0, 2)  negative_user_withdraw_score,
bound_data(negative_complaints_rate*50, 0, 5) negative_complaint_score, 
bound_data(negative_bad_rating_rate*40, 0, 3) negative_bad_rate_score,
bound_data(reminder_rate*20,0, 2) negative_reminder_score 
from test.temp_restaurant_for_zf_zyl t1 
join (select city_name, count(*) num from test.temp_restaurant_for_zf_zyl group by city_name) t2 
on (t1.city_name=t2.city_name);

drop table temp.temp_restaurant_city_score_max_min;
create table temp.temp_restaurant_city_score_max_min as
select city_name, 
max(pinpai_score) as pinpai_score_max, min(pinpai_score) as pinpai_score_min,
max(work_day_score) as work_day_score_max, min(work_day_score) as work_day_score_min,
max(user_score) as user_score_max, min(user_score) as user_score_min,
max(back_user_score1) as back_user_score1_max, min(back_user_score1) as back_user_score1_min,
max(order_score) as order_score_max, min(order_score) as order_score_min,
max(price_score) as price_score_max, min(price_score) as price_score_min,
max(collection_score) as collection_score_max, min(collection_score) as collection_score_min,
max(negative_restaurant_withdraw_score) as negative_restaurant_withdraw_score_max, min(negative_restaurant_withdraw_score) as negative_restaurant_withdraw_score_min,
max(negative_user_withdraw_score) as negative_user_withdraw_score_max, min(negative_user_withdraw_score) as negative_user_withdraw_score_min,
max(negative_complaint_score) as negative_complaint_score_max, min(negative_complaint_score) as negative_complaint_score_min,
max(negative_bad_rate_score) as negative_bad_rate_score_max, min(negative_bad_rate_score) as negative_bad_rate_score_min,
max(negative_reminder_score) as negative_reminder_score_max, min(negative_reminder_score) as negative_reminder_score_min
from temp.temp_restaurant_score_for_zf
group by city_name;

drop table temp.temp_restaurant_score_for_zf_normalize;
create table temp.temp_restaurant_score_for_zf_normalize as 
select t1.restaurant_id, t1.restaurant_name, t1.city_name,
10 * case when (pinpai_score_max - pinpai_score_min)!=0.0 then (pinpai_score - pinpai_score_min)/(pinpai_score_max - pinpai_score_min) else 0.0 end as pinpai_score_normal,
10 * case when (work_day_score_max - work_day_score_min)!=0.0 then (work_day_score - work_day_score_min)/(work_day_score_max - work_day_score_min) else 0.0 end as work_day_score_normal,
10 * case when (user_score_max - user_score_min)!=0.0 then (user_score - user_score_min)/(user_score_max - user_score_min) else 0.0 end as user_score_normal,
10 * case when (back_user_score1_max - back_user_score1_min)!=0.0 then (back_user_score1 - back_user_score1_min)/(back_user_score1_max - back_user_score1_min) else 0.0 end as back_user_score1_normal,
10 * case when (order_score_max - order_score_min)!=0.0 then (order_score - order_score_min)/(order_score_max - order_score_min) else 0.0 end as order_score_normal,
10 * case when (price_score_max - price_score_min)!=0.0 then (price_score - price_score_min)/(price_score_max - price_score_min) else 0.0 end as price_score_normal,
10 * case when (collection_score_max - collection_score_min)!=0.0 then (collection_score - collection_score_min)/(collection_score_max - collection_score_min) else 0.0 end as collection_score_normal,
10 * case when (negative_restaurant_withdraw_score_max - negative_restaurant_withdraw_score_min)!=0.0 then (negative_restaurant_withdraw_score - negative_restaurant_withdraw_score_min)/(negative_restaurant_withdraw_score_max - negative_restaurant_withdraw_score_min) else 0.0 end as negative_restaurant_withdraw_score_normal,
10 * case when (negative_user_withdraw_score_max - negative_user_withdraw_score_min)!=0.0 then (negative_user_withdraw_score - negative_user_withdraw_score_min)/(negative_user_withdraw_score_max - negative_user_withdraw_score_min) else 0.0 end as negative_user_withdraw_score_normal,
10 * case when (negative_complaint_score_max - negative_complaint_score_min)!=0.0 then (negative_complaint_score - negative_complaint_score_min)/(negative_complaint_score_max - negative_complaint_score_min) else 0.0 end as negative_complaint_score_normal,
10 * case when (negative_bad_rate_score_max - negative_bad_rate_score_min)!=0.0 then (negative_bad_rate_score - negative_bad_rate_score_min)/(negative_bad_rate_score_max - negative_bad_rate_score_min) else 0.0 end as negative_bad_rate_score_normal,
10 * case when (negative_reminder_score_max - negative_reminder_score_min)!=0.0 then (negative_reminder_score - negative_reminder_score_min)/(negative_reminder_score_max - negative_reminder_score_min) else 0.0 end as negative_reminder_score_normal
from temp.temp_restaurant_score_for_zf t1
join temp.temp_restaurant_city_score_max_min t2
on t1.city_name=t2.city_name;

drop table temp.temp_restaurant_total_score_for_zf;
create table temp.temp_restaurant_total_score_for_zf as 
select *, 
case when t.total_score>21 then 'A' 
when t.total_score>15 then 'B'
when t.total_score>11.5 then 'C'
when t.total_score>5 then 'D'
else 'E' end as rank
from(
select restaurant_id, restaurant_name, city_name, 
round(pinpai_score_normal,2), round(work_day_score_normal,2), round(back_user_score1_normal,2), round(price_score_normal,2),
round(collection_score_normal,2), round(negative_restaurant_withdraw_score_normal,2), round(negative_user_withdraw_score_normal,2),
round(negative_complaint_score_normal,2), round(negative_bad_rate_score_normal,2), round(negative_reminder_score_normal,2),
round(pinpai_score_normal + work_day_score_normal + user_score_normal + back_user_score1_normal + price_score_normal + collection_score_normal
- negative_restaurant_withdraw_score_normal - negative_user_withdraw_score_normal - negative_complaint_score_normal 
- negative_bad_rate_score_normal - negative_reminder_score_normal,2) as total_score
from temp.temp_restaurant_score_for_zf_normalize
) t;
select rank, count(*) from temp_restaurant_total_score_for_zf group by rank;

select back_user_score from temp.temp_restaurant_score_for_zf limit 100;
select floor(collection_score), count(*) from test.temp_restaurant_score_for_zf group by floor(collection_score);




