TRUNCATE TABLE $login__DWH.$table_name;
INSERT INTO $login__DWH.$table_name($columns)
select 
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.event_dt,
    now(),
    's3'
from $login__STAGING.group_log as gl
left join $login__DWH.h_groups as hg on gl.group_id = hg.group_id
left join $login__DWH.h_users as hu on gl.user_id = hu.user_id
left join $login__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;