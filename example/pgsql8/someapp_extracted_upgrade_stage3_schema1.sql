-- pgsql8/someapp_extracted_upgrade_stage3_schema1.sql
-- DBSteward stage 3 structure changes, constraints, and removals - generated Sun, 20 Jun 2021 18:47:04 +0000
-- Old definition: pgsql8/someapp_v2_composite.xml
-- New definition pgsql8/someapp_extracted_composite.xml

BEGIN;

DROP SCHEMA search_results CASCADE;

ALTER TABLE public.group_list
  ALTER COLUMN group_list_id SET NOT NULL;

ALTER TABLE public.user_access_level
  ALTER COLUMN user_access_level_id SET NOT NULL;

ALTER TABLE public.user_status_list
  ALTER COLUMN user_status_list_id SET NOT NULL;

ALTER TABLE public.sql_user
  ALTER COLUMN user_id SET NOT NULL;

