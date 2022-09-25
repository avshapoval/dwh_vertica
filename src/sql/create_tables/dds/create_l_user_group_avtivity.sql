CREATE TABLE AVSHAPOWALYANDEXRU__DWH.l_user_group_activity(
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id int NOT NULL REFERENCES AVSHAPOWALYANDEXRU__DWH.h_users(hk_user_id),
	hk_group_id int NOT NULL REFERENCES AVSHAPOWALYANDEXRU__DWH.h_groups(hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
segmented BY hk_l_user_group_activity ALL nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);