-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Enrich Data and Add Intermediate tables
-- MAGIC

-- COMMAND ----------

--Create Table for Operating Companies and add desctiptions
drop table if exists dev_db.intermediate.active_companies;
create table if not exists dev_db.intermediate.active_companies as
select
  a.uuid as org_uuid,
  a.name,
  a.type as org_type,
  a.country_code,
  a.homepage_url,
  a.employee_count,
  a.status,
  b.description,
  current_timestamp() as ingestion_date
from
  dev_db.raw.organizations a
  left join dev_db.raw.organization_descriptions b on a.uuid = b.uuid
where
  a.status in('operating','ipo');

-- COMMAND ----------

--Create table for investors
drop table if exists dev_db.intermediate.active_investors;
create table if not exists dev_db.intermediate.active_investors as
select
  uuid investor_uuid,
  name investor_name,
  domain,
  country_code,
  type as investor_type,
  current_timestamp() as ingestion_date
from
  dev_db.raw.investors;

-- COMMAND ----------

--Create table for investments associated with funding rounds
drop table if exists dev_db.intermediate.investments_funding;
create table if not exists dev_db.intermediate.investments_funding as with cte_funding_rounds as (
  --Get list of funding rounds
  select
    uuid as funding_round_uuid,
    org_uuid,
    raised_amount_usd,
    announced_on
  from
    dev_db.raw.funding_rounds
),
cte_funding_investments as (
  select
    funding_round_uuid,
    array_join(collect_set(name), ', ') investments
  from
    dev_db.raw.investments
  group by
    1
)
select
  a.funding_round_uuid,
  org_uuid,
  raised_amount_usd,
  announced_on,
  b.investments,
  current_timestamp() as ingestion_date
from
  cte_funding_rounds a
  left join cte_funding_investments b on a.funding_round_uuid = b.funding_round_uuid;
