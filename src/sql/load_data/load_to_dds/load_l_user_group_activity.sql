TRUNCATE TABLE $login__DWH.$table_name;
INSERT INTO $login__DWH.$table_name($columns)
select distinct
        hash(hu.hk_user_id, hg.hk_group_id),
        hu.hk_user_id, 
        hg.hk_group_id,
        now(),
        's3'
from $login__STAGING.group_log as gl
left join $login__DWH.h_users as hu on gl.user_id = hu.user_id
left join $login__DWH.h_groups as hg on gl.group_id = hg.group_id
;