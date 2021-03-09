CREATE TABLE public.advertiser (
    device_id numeric(3,0)  NOT NULL,
    name character varying  NOT NULL,
    booking_country character varying  NOT NULL,
    id numeric  NOT NULL,
    last_updated character varying  NOT NULL,
    status character varying  NOT NULL,
    currency character varying  NOT NULL,
    managed_by numeric  NOT NULL,
CONSTRAINT advertiser_pkey
    PRIMARY KEY (id));
CREATE TABLE public.campaign (
    device_id numeric(3,0)  NOT NULL,
    campaign_name character varying  NOT NULL,
    id numeric  NOT NULL,
    advertiser_id numeric  NULL,
    status character varying  NOT NULL,
    campaign_total character varying  NOT NULL,
    campaign_start_date character varying  NOT NULL,
    campaign_end_date character varying  NOT NULL,
CONSTRAINT campaign_pkey
    PRIMARY KEY (id),
CONSTRAINT campaign_advertiser_id_fkey
    FOREIGN KEY (advertiser_id) REFERENCES advertiser(id));
CREATE TABLE public.ad_group (
    device_id numeric(3,0)  NOT NULL,
    name character varying  NOT NULL,
    column2_id numeric  NOT NULL,
    id numeric  NOT NULL,
    advertiser_id numeric  NULL,
    status character varying  NOT NULL,
    campaign_id numeric  NOT NULL,
CONSTRAINT ad_group_pkey
    PRIMARY KEY (id),
CONSTRAINT ad_group_advertiser_id_fkey
    FOREIGN KEY (advertiser_id) REFERENCES advertiser(id),
CONSTRAINT ad_group_campaign_id_fkey
    FOREIGN KEY (campaign_id) REFERENCES campaign(id));
CREATE TABLE public.campaign_stats (
    impressions numeric(3,0) DEFAULT 1 NOT NULL,
    spend numeric DEFAULT 0.0 NOT NULL,
    stats_date date  NOT NULL,
    advertiser_id numeric  NOT NULL,
    clicks numeric(3,0) DEFAULT 0 NOT NULL,
    campaign_id numeric  NOT NULL,
CONSTRAINT campaign_stats_advertiser_id_fkey
    FOREIGN KEY (advertiser_id) REFERENCES advertiser(id),
CONSTRAINT campaign_stats_campaign_id_fkey
    FOREIGN KEY (campaign_id) REFERENCES campaign(id));
CREATE TABLE public.targetingattribute (
    device_id numeric(3,0)  NOT NULL,
    created_date date  NOT NULL,
    parent_id numeric  NOT NULL,
    landing_url character varying(2048)  NOT NULL,
    created_by_user character varying(255)  NOT NULL,
    ad_param_value_2 character varying(200)  NOT NULL,
    hidden numeric  NOT NULL,
    ad_param_value_3 character varying(200)  NOT NULL,
    cpc numeric  NOT NULL,
    editorial_results character varying(256)  NOT NULL,
    ad_param_value_1 character varying(2048)  NOT NULL,
    id numeric  NOT NULL,
    parent_type character varying(64)  NOT NULL,
    last_updated date  NOT NULL,
    advertiser_id numeric  NULL,
    status character varying(255)  NOT NULL,
    modifier numeric  NOT NULL,
    deleted_date date  NOT NULL,
    last_updated_by_user character varying(255)  NOT NULL,
    match_type character varying(64)  NOT NULL,
    value character varying(255)  NOT NULL,
CONSTRAINT targetingattribute_pkey
    PRIMARY KEY (id),
CONSTRAINT targetingattribute_advertiser_id_fkey
    FOREIGN KEY (advertiser_id) REFERENCES advertiser(id),
CONSTRAINT targetingattribute_ad_group_id_fkey
    FOREIGN KEY (parent_id) REFERENCES ad_group(id));
CREATE TABLE public.ad (
    impressions numeric  NOT NULL,
    device_id numeric(3,0)  NOT NULL,
    ad_group_id numeric  NOT NULL,
    id numeric  NOT NULL,
    user_count numeric  NOT NULL,
    advertiser_id numeric  NULL,
    status character varying  NOT NULL,
    title character varying  NOT NULL,
    campaign_id numeric  NOT NULL,
CONSTRAINT ad_pkey
    PRIMARY KEY (id),
CONSTRAINT ad_advertiser_id_fkey
    FOREIGN KEY (advertiser_id) REFERENCES advertiser(id),
CONSTRAINT ad_campaign_id_fkey
    FOREIGN KEY (campaign_id) REFERENCES campaign(id),
CONSTRAINT ad_ad_group_id_fkey
    FOREIGN KEY (ad_group_id) REFERENCES ad_group(id));

CREATE OR REPLACE FUNCTION update_last_updated_column() RETURNS TRIGGER AS $BODY$ BEGIN   NEW.last_updated = now();   RETURN NEW;END;  $BODY$ LANGUAGE plpgsql;
DROP VIEW IF EXISTS account_stats;
CREATE VIEW account_stats AS
 SELECT campaign_stats.stats_date,
    campaign_stats.advertiser_id,
    sum(campaign_stats.impressions) AS impressions,
    sum(campaign_stats.clicks) AS clicks,
    sum(campaign_stats.spend) AS spend
   FROM campaign_stats
  GROUP BY campaign_stats.stats_date, campaign_stats.advertiser_id;

DROP TRIGGER IF EXISTS update_targetingattribute_last_updated ON targetingattribute;
CREATE TRIGGER update_targetingattribute_last_updated BEFORE UPDATE ON targetingattribute
    FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();
