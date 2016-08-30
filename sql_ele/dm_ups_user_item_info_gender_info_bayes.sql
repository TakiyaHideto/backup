#***************************************************************************************************
# ** 文件名称： dm_mdl_predict_sex_prob_via_name.sql
# ** 功能描述： 预测男女性别
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-26
#***************************************************************************************************

SET 
    mapred.max.split.size=1000000000;
DROP TABLE 
    temp.temp_single_word_sex_frq;
CREATE TABLE 
    temp.temp_single_word_sex_frq AS
SELECT 
    t.name_char, t.sex, count(*) as frq
FROM(
    SELECT 
        case when length(get_normal_word(t.name,0))>0 then single_word else t.name end as name_char, 
        sex
    FROM(
        SELECT 
            name, sex 
        FROM 
            dw.dw_usr_address 
        WHERE 
            dt>='2016-01-01' 
        GROUP BY 
            name, sex
        ) t
    LATERAL VIEW EXPLODE(split(name,'|')) tmp AS single_word
    ) t
WHERE 
    t.name_char!='' and t.name_char is not null and (t.sex='1' or t.sex='2')
GROUP BY 
    t.name_char, t.sex;


DROP TABLE 
    temp.temp_single_word_sex_prob;
CREATE TABLE 
    temp.temp_single_word_sex_prob AS
SELECT 
    name_char, sum(case when sex='1' then frq else 0 end)/sum(frq) as male_prob, sum(case when sex='2' then frq else 0 end)/sum(frq) as female_prob
FROM 
    temp.temp_single_word_sex_frq
GROUP BY 
    name_char;


DROP TABLE 
    temp.temp_test_sex_probability;
CREATE TABLE 
    temp.temp_test_sex_probability AS
SELECT 
    t1.user_id, t1.name, sum(ln(t2.male_prob)) as male_prob, sum(ln(t2.female_prob)) as female_prob
FROM(
    SELECT 
        user_id, t.name, t.name_char, t.sex
    FROM(
        SELECT 
            user_id, name, case when length(get_normal_word(t.name,0))>0 then single_word else t.name end as name_char, sex
        FROM(
            SELECT 
                user_id, name, sex 
            FROM 
                dw.dw_usr_address 
            WHERE 
                dt>='2016-01-01' 
            GROUP BY 
                user_id, name, sex
            ) t
        LATERAL VIEW EXPLODE(split(name,'|')) tmp AS single_word
        WHERE 
            length(single_word)>0 or length(name)>0
        ) t
    WHERE 
        t.name_char!='' and t.name_char is not null and (t.sex='0' or t.sex is null)
    ) t1
JOIN 
    temp.temp_single_word_sex_prob t2
ON 
    t1.name_char=t2.name_char
WHERE 
    length(t2.name_char)>0
GROUP BY 
    t1.user_id, t1.name;

SET 
    mapred.max.split.size=1000000000;
DROP TABLE 
    temp.temp_user_sexuality_table;
CREATE TABLE 
    temp.temp_user_sexuality_table AS
SELECT t.user_id, t.name, t.sex
FROM(
    SELECT 
        user_id, name, concat(case when male_prob>female_prob then 1 else 2 end, '_2') as sex
    FROM 
        temp.temp_test_sex_probability
    UNION ALL
    SELECT 
        user_id, name, concat(sex, '_1') as sex
    FROM 
        dw.dw_usr_address
    WHERE 
        dt>='2016-01-01' and name!='' and name is not null and (sex='1' or sex='2')
) t
GROUP BY t.user_id, t.name, t.sex;



INSERT OVERWRITE TABLE 
    dm.dm_ups_user_item_info PARTITION(dt='${day}', flag='base_gender')
SELECT 
    user_id, 'base' AS top_category, 'gender' AS attr_key, sex AS attr_value, 0 AS is_json, '${day}' AS update_time
FROM
    temp.temp_user_sexuality_table
WHERE
    dt='${day}';

INSERT OVERWRITE TABLE 
    dm.dm_ups_user_item_info PARTITION(dt='3000-12-31', flag='base_gender')
SELECT 
    user_id, 'base' AS top_category, 'gender' AS attr_key, sex AS attr_value, 0 AS is_json, '${day}' AS update_time
FROM
    temp.temp_user_sexuality_table
WHERE
    dt='${day}';







