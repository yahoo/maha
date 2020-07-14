CREATE TABLE advertiser_postgres
(device_id	NUMERIC(3)		NOT NULL, 
name	VARCHAR		NOT NULL, 
booking_country	VARCHAR		NOT NULL, 
id	NUMERIC		NOT NULL, 
last_updated	VARCHAR		NOT NULL, 
status	VARCHAR		NOT NULL, 
currency	VARCHAR		NOT NULL, 
managed_by	NUMERIC		NOT NULL)
;

     
CREATE TABLE campaign_postgres
(device_id	NUMERIC(3)		NOT NULL, 
campaign_name	VARCHAR		NOT NULL, 
id	NUMERIC		NOT NULL, 
advertiser_id	NUMERIC		NULL, 
status	VARCHAR		NOT NULL, 
campaign_total	VARCHAR		NOT NULL, 
campaign_start_date	VARCHAR		NOT NULL, 
campaign_end_date	VARCHAR		NOT NULL)
;

     
CREATE TABLE ad_group_postgres
(device_id	NUMERIC(3)		NOT NULL, 
name	VARCHAR		NOT NULL, 
column2_id	NUMERIC		NOT NULL, 
id	NUMERIC		NOT NULL, 
advertiser_id	NUMERIC		NULL, 
status	VARCHAR		NOT NULL, 
campaign_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE pg_targetingattribute
(device_id	NUMERIC(3)		NOT NULL, 
created_date	DATE		NOT NULL, 
parent_id	NUMERIC		NOT NULL, 
landing_url	VARCHAR(2048)		NOT NULL, 
created_by_user	VARCHAR(255)		NOT NULL, 
ad_param_value_2	VARCHAR(200)		NOT NULL, 
hidden	NUMERIC		NOT NULL, 
ad_param_value_3	VARCHAR(200)		NOT NULL, 
cpc	NUMERIC		NOT NULL, 
editorial_results	VARCHAR(256)		NOT NULL, 
ad_param_value_1	VARCHAR(2048)		NOT NULL, 
id	NUMERIC		NOT NULL, 
parent_type	VARCHAR(64)		NOT NULL, 
last_updated	DATE		NOT NULL, 
advertiser_id	NUMERIC		NULL, 
status	VARCHAR(255)		NOT NULL, 
modifier	NUMERIC		NOT NULL, 
deleted_date	DATE		NOT NULL, 
last_updated_by_user	VARCHAR(255)		NOT NULL, 
match_type	VARCHAR(64)		NOT NULL, 
value	VARCHAR(255)		NOT NULL)
;

     
CREATE TABLE pg_non_hash_paritioned_with_singleton_dim
(id	NUMERIC		NOT NULL, 
name	VARCHAR		NOT NULL, 
status	VARCHAR		NOT NULL)
;

     
CREATE TABLE ad_dim_postgres
(impressions	NUMERIC		NOT NULL, 
device_id	NUMERIC(3)		NOT NULL, 
ad_group_id	NUMERIC		NOT NULL, 
id	NUMERIC		NOT NULL, 
user_count	NUMERIC		NOT NULL, 
advertiser_id	NUMERIC		NULL, 
status	VARCHAR		NOT NULL, 
title	VARCHAR		NOT NULL, 
campaign_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE restaurant_postgres
(id	NUMERIC		NOT NULL, 
address	VARCHAR(1000)		NOT NULL)
;

     
CREATE TABLE pg_combined_class
(id	NUMERIC		NOT NULL, 
address	VARCHAR(1000)		NOT NULL)
;

     
