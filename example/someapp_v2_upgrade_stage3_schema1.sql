-- example/someapp_v2_upgrade_stage3_schema1.sql
-- DBSteward stage 3 structure changes, constraints, and removals - generated Tue, 19 Jan 2021 21:20:58 -0500
-- Old definition: example/someapp_v1_composite.xml
-- New definition example/someapp_v2_composite.xml

BEGIN;

ALTER TABLE public.group_list
  ALTER COLUMN group_description SET NOT NULL;

ALTER TABLE public.sql_user
  ALTER COLUMN user_access_level_id SET NOT NULL;

-- sql_user DROP COLUMN somecol omitted: new column somecol1 indicates it is the replacement for somecol;

GRANT SELECT ON TABLE public.group_list_view TO someapp;


-- SQL STAGE STAGE3 COMMANDS


COMMIT;
