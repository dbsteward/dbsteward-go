-- someapp_extracted_upgrade_stage3_schema1.sql
-- DBSteward stage 3 structure changes, constraints, and removals - generated Mon, 01 Feb 2021 13:34:20 -0500
-- Old definition: someapp_v2_composite.xml
-- New definition someapp_extracted_composite.xml

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

