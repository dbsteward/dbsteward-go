-- example/someapp_extracted_upgrade_stage1_schema1.sql
-- DBSteward stage 1 structure additions and modifications - generated Tue, 26 Jan 2021 15:13:16 -0500
-- Old definition: example/someapp_v2_composite.xml
-- New definition example/someapp_extracted_composite.xml

BEGIN;


-- SQL STAGE STAGE1BEFORE COMMANDS

DROP VIEW IF EXISTS public.group_list_view;

