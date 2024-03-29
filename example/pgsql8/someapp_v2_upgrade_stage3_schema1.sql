-- pgsql8/someapp_v2_upgrade_stage3_schema1.sql
-- DBSteward stage 3 structure changes, constraints, and removals - generated Sun, 17 Oct 2021 11:46:25 -0400
-- Old definition: pgsql8/someapp_v1_composite.xml
-- New definition pgsql8/someapp_v2_composite.xml

BEGIN;

ALTER TABLE public.group_list
  ALTER COLUMN group_description SET NOT NULL;

ALTER TABLE public.sql_user
  ALTER COLUMN user_access_level_id SET NOT NULL;

-- sql_user DROP COLUMN somecol omitted: new column somecol1 indicates it is the replacement for somecol;

CREATE OR REPLACE VIEW public.group_list_view AS
  SELECT * FROM public.group_list WHERE group_deleted = FALSE AND group_visible = TRUE;

ALTER VIEW public.group_list_view OWNER TO pgsql;

GRANT SELECT ON TABLE public.group_list_view TO someapp;


-- SQL STAGE STAGE3 COMMANDS


COMMIT;
