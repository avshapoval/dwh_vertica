WITH user_group_log AS (
	SELECT 
		hg.hk_group_id, 
		count(DISTINCT luga.hk_user_id) AS cnt_added_users
	FROM AVSHAPOWALYANDEXRU__DWH.l_user_group_activity luga 
	INNER JOIN AVSHAPOWALYANDEXRU__DWH.s_auth_history sah ON luga.hk_l_user_group_activity  = sah.hk_l_user_group_activity
	INNER JOIN AVSHAPOWALYANDEXRU__DWH.h_groups hg ON luga.hk_group_id = hg.hk_group_id 
	WHERE event = 'add'
	GROUP BY hg.hk_group_id, hg.registration_dt 
	ORDER BY hg.registration_dt ASC
	LIMIT 10
),
first_msg_by_groups as (
    SELECT
    	lgd.hk_group_id, 
		count(DISTINCT hu.hk_user_id) count_msg
    FROM AVSHAPOWALYANDEXRU__DWH.l_groups_dialogs lgd 
    INNER JOIN AVSHAPOWALYANDEXRU__DWH.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id 
    INNER JOIN AVSHAPOWALYANDEXRU__DWH.h_users hu ON lum.hk_user_id = hu.hk_user_id 
    GROUP BY lgd.hk_group_id 
),
user_group_messages AS (
	SELECT
		ugl.hk_group_id,
		ugl.cnt_added_users,
		fmbg.count_msg AS cnt_users_in_group_with_messages,
		fmbg.count_msg/NULLIF(ugl.cnt_added_users, 0) AS  group_conversion
	FROM first_msg_by_groups fmbg 
	INNER JOIN user_group_log ugl ON ugl.hk_group_id = fmbg.hk_group_id
)
select 
	hk_group_id, 
	cnt_added_users,
	cnt_users_in_group_with_messages,
	group_conversion
from user_group_messages
order by group_conversion DESC;
