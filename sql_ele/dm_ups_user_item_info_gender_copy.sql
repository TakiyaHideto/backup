#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_user_item_info_gender.sql
# **  功能描述：
#        1. 从用户的收货地址中提取填写的性别信息
#
# **  创建者：weihua.zheng@ele.me
# **  创建日期： 2016-08-04 10:10:00
# **
# **  ChangeLog：
#
#***************************************************************************************************

##### sub task : 1
##### 从用户的收货地址中（daily）提取性别的信息一致（地址中可能有多个性别不一致的情况）的用户性别信息
##### 然后和历史累积的数据进行合并，同一个用户以最新的信息为准。

INSERT OVERWRITE TABLE rec.rec_ups_midd_user_gender_info PARTITION(dt = '${day}')
SELECT
    d.user_id,
    d.sex,
    d.update_dt
FROM
(
    SELECT
        c.user_id,
        c.sex,
        c.update_dt,
        ROW_NUMBER() OVER (PARTITION BY c.user_id ORDER BY c.update_dt DESC) AS row_num
    FROM
    (
        ---- 得到性别一致的用户信息
        SELECT
            b.user_id,
            b.sex,
            '${day}' AS update_dt
        FROM
        (
            SELECT
                a.user_id,
                a.sex,
                COUNT(*) OVER (PARTITION BY a.user_id) AS sex_sum
            FROM
            (
                ---- 排重用户的性别信息
                SELECT
                    user_id, sex
                FROM
                    ods.ods_address
                WHERE
                    dt = '${day}' AND sex > 0
                GROUP BY
                    user_id, sex
            ) a
        ) b
        WHERE
            b.sex_sum = 1

        UNION ALL

        ---- 历史累积用户信息，即前一天的全量用户信息
        SELECT
            user_id,
            gender AS sex,
            update_dt
        FROM
            rec.rec_ups_midd_user_gender_info
        WHERE
            dt = GET_DATE('${day}', -1)
    ) c
) d
WHERE
    d.row_num = 1;


##### sub task 2
##### 插入用户性别信息到用户画像基础表

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '${day}', flag = 'base_gender')
SELECT
    user_id,
    'base' AS top_category,
    'gender' AS attr_key,
    gender AS attr_value,
    0 AS is_json,
    update_dt AS update_time
FROM
   rec.rec_ups_midd_user_gender_info
WHERE
   dt = '${day}';


##### sub task 3
##### copy newest data to dt = '3000-12-31'

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '3000-12-31', flag = 'base_gender')
SELECT
    user_id, top_category, attr_key, attr_value, is_json, update_time
FROM
   dm.dm_ups_user_item_info
WHERE
   dt = '${day}' AND flag = 'base_gender';

