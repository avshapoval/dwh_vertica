CREATE TABLE AVSHAPOWALYANDEXRU__STAGING.group_log(
	group_id int NOT NULL,
	user_id int NOT NULL,
	user_id_from int,
	event varchar(6),
	event_dt timestamp(0)
)
ORDER BY event_dt
segmented BY hash(group_id) ALL nodes
PARTITION BY event_dt
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);
