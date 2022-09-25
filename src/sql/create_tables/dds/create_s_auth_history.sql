CREATE TABLE AVSHAPOWALYANDEXRU__DWH.s_auth_history(
	hk_l_user_group_activity int NOT NULL REFERENCES AVSHAPOWALYANDEXRU__DWH.l_user_group_activity(hk_l_user_group_activity),
	user_id_from int references AVSHAPOWALYANDEXRU__DWH.h_users(hk_user_id),
	event varchar(6),
	event_dt timestamp(0),
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
segmented BY hk_l_user_group_activity ALL nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);