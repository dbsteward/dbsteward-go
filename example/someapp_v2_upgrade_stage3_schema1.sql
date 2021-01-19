-- example/someapp_v2_upgrade_stage3_schema1.sql
-- DBSteward stage 3 structure changes, constraints, and removals - generated Tue, 19 Jan 2021 14:30:46 -0500
-- Old definition: example/someapp_v1_composite.xml
-- New definition example/someapp_v2_composite.xml

BEGIN;

GRANT SELECT ON TABLE public.group_list_view TO someapp;


-- SQL STAGE STAGE3 COMMANDS

