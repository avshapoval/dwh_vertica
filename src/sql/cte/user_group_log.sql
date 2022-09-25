WITH user_group_log AS (
	SELECT 
		hg.hk_group_id, 
		count(DISTINCT luga.hk_user_id) cnt_added_users,
		hg.registration_dt 
	FROM AVSHAPOWALYANDEXRU__DWH.l_user_group_activity luga 
	INNER JOIN AVSHAPOWALYANDEXRU__DWH.s_auth_history sah ON luga.hk_l_user_group_activity  = sah.hk_l_user_group_activity
	INNER JOIN AVSHAPOWALYANDEXRU__DWH.h_groups hg ON luga.hk_group_id = hg.hk_group_id 
	WHERE event = 'add'
	GROUP BY hg.hk_group_id, hg.registration_dt 
	ORDER BY hg.registration_dt ASC
	LIMIT 10
)
select 
	 hk_group_id,
     cnt_added_users, 
     registration_dt
from user_group_log
order by cnt_added_users
limit 10;