CREATE TABLE fact2
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
device_id	NUMERIC(3)		NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
min_col	NUMERIC		NULL, 
column2_id	NUMERIC		NOT NULL, 
target_page_url	VARCHAR		NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
landing_page_url	VARCHAR		NOT NULL, 
network_type	VARCHAR(100)		NOT NULL, 
stats_date	DATE		NOT NULL, 
pricing_type	NUMERIC(3)		NOT NULL, 
max_col	NUMERIC		NULL, 
avg_col	NUMERIC		NULL, 
ad_group_id	NUMERIC		NOT NULL, 
count_col	NUMERIC		NULL, 
stats_source	NUMERIC(3)		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
average_cpc	NUMERIC		NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
ctr	NUMERIC		NULL, 
campaign_id	NUMERIC		NOT NULL, 
keyword_id	NUMERIC		NOT NULL, 
column_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE fact1
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
device_id	NUMERIC(3)		NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
min_col	NUMERIC		NULL, 
column2_id	NUMERIC		NOT NULL, 
target_page_url	VARCHAR		NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
landing_page_url	VARCHAR		NOT NULL, 
network_type	VARCHAR(100)		NOT NULL, 
stats_date	DATE		NOT NULL, 
price_type	NUMERIC(3)		NOT NULL, 
max_col	NUMERIC		NULL, 
avg_col	NUMERIC		NULL, 
ad_group_id	NUMERIC		NOT NULL, 
count_col	NUMERIC		NULL, 
stats_source	NUMERIC(3)		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
average_cpc	NUMERIC		NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
ctr	NUMERIC		NULL, 
ad_id	NUMERIC		NOT NULL, 
campaign_id	NUMERIC		NOT NULL, 
keyword_id	NUMERIC		NOT NULL, 
column_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE k_stats_fact1
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
column2_id	NUMERIC		NOT NULL, 
utc_date	DATE		NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
landing_page_url	VARCHAR		NOT NULL, 
stats_date	DATE		NOT NULL, 
pricing_type	NUMERIC(3)		NOT NULL, 
ad_group_id	NUMERIC		NOT NULL, 
stats_source	NUMERIC(3)		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
average_cpc	NUMERIC		NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
ctr	NUMERIC		NULL, 
frequency	VARCHAR		NULL, 
campaign_id	NUMERIC		NOT NULL, 
keyword_id	NUMERIC		NOT NULL, 
column_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE ad_fact1
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
min_col	NUMERIC		NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
stats_date	DATE		NOT NULL, 
price_type	NUMERIC(3)		NOT NULL, 
user_count	NUMERIC		NULL, 
max_col	NUMERIC		NULL, 
avg_col	NUMERIC(6)	DEFAULT 0	NOT NULL, 
ad_group_id	NUMERIC		NOT NULL, 
count	NUMERIC		NULL, 
stats_source	NUMERIC(3)		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
s_impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
ctr	NUMERIC		NULL, 
ad_id	NUMERIC		NOT NULL, 
show_flag	NUMERIC		NOT NULL, 
custom_col	NUMERIC(6)	DEFAULT 0	NOT NULL, 
campaign_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE k_stats_new_partitioning_one
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
column2_id	NUMERIC		NOT NULL, 
utc_date	DATE		NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
landing_page_url	VARCHAR		NOT NULL, 
constantfact	NUMERIC(3)	DEFAULT 0	NOT NULL, 
stats_date	DATE		NOT NULL, 
price_type	NUMERIC(3)		NOT NULL, 
ad_group_id	NUMERIC		NOT NULL, 
stats_source	NUMERIC(3)		NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
average_cpc	NUMERIC		NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
ctr	NUMERIC		NULL, 
ad_id	NUMERIC		NOT NULL, 
frequency	VARCHAR		NULL, 
campaign_id	NUMERIC		NOT NULL, 
keyword_id	NUMERIC		NOT NULL, 
column_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE k_stats_new_partitioning_two
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
column2_id	NUMERIC		NOT NULL, 
utc_date	DATE		NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
landing_page_url	VARCHAR		NOT NULL, 
constantfact	NUMERIC(3)	DEFAULT 0	NOT NULL, 
stats_date	DATE		NOT NULL, 
price_type	NUMERIC(3)		NOT NULL, 
ad_group_id	NUMERIC		NOT NULL, 
stats_source	NUMERIC(3)		NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
average_cpc	NUMERIC		NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
ctr	NUMERIC		NULL, 
ad_id	NUMERIC		NOT NULL, 
frequency	VARCHAR		NULL, 
campaign_id	NUMERIC		NOT NULL, 
keyword_id	NUMERIC		NOT NULL, 
column_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE campaign_stats
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
stats_date	DATE		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
campaign_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE campaign_adjustments
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
stats_date	DATE		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
campaign_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE account_stats
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
stats_date	DATE		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL)
;

     
CREATE TABLE account_adjustment
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
stats_date	DATE		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL)
;

     
CREATE TABLE v_publisher_stats
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
publisher_id	NUMERIC		NOT NULL, 
date_sid	NUMERIC		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL)
;

     
CREATE TABLE v_publisher_stats_str
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
publisher_id	NUMERIC		NOT NULL, 
date_sid	VARCHAR		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL)
;

     
CREATE TABLE v_publisher_stats2
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
publisher_id	NUMERIC		NOT NULL, 
date_sid	NUMERIC		NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL)
;

     
CREATE TABLE fact_druid
(impressions	NUMERIC(3)	DEFAULT 1	NOT NULL, 
avg_pos	NUMERIC	DEFAULT 0.0	NOT NULL, 
start_time	NUMERIC		NOT NULL, 
column2_id	NUMERIC		NOT NULL, 
spend	NUMERIC	DEFAULT 0.0	NOT NULL, 
landing_page_url	VARCHAR		NOT NULL, 
stats_date	DATE		NOT NULL, 
price_type	NUMERIC(3)		NOT NULL, 
ad_group_id	NUMERIC		NOT NULL, 
id	NUMERIC		NOT NULL, 
stats_source	NUMERIC(3)		NOT NULL, 
advertiser_id	NUMERIC		NOT NULL, 
max_bid	NUMERIC	DEFAULT 0.0	NOT NULL, 
clicks	NUMERIC(3)	DEFAULT 0	NOT NULL, 
country_woeid	NUMERIC		NOT NULL, 
campaign_id	NUMERIC		NOT NULL, 
column_id	NUMERIC		NOT NULL)
;

     
CREATE TABLE f_class_stats
(class_id	NUMERIC		NOT NULL, 
class_name	NUMERIC(10)		NOT NULL, 
date	DATE		NOT NULL, 
num_students	NUMERIC		NULL)
;

     
