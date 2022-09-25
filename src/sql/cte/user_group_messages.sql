WITH added_by_groups AS (
	SELECT 
		hk_group_id, 
		count(DISTINCT hk_user_id) count_added
	FROM AVSHAPOWALYANDEXRU__DWH.l_user_group_activity luga 
	INNER JOIN AVSHAPOWALYANDEXRU__DWH.s_auth_history sah ON luga.hk_l_user_group_activity  = sah.hk_l_user_group_activity 
	WHERE event = 'add'
	GROUP BY hk_group_id
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
		abg.hk_group_id,
		count_msg/NULLIF(count_added, 0) cnt_users_in_group_with_messages
	FROM first_msg_by_groups fmbg 
	LEFT JOIN added_by_groups abg ON abg.hk_group_id = fmbg.hk_group_id
)
select 
	hk_group_id, 
	cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;
