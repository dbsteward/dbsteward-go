-- pgsql8/someapp_extracted_upgrade_stage3_schema1.sql
-- DBSteward stage 3 structure changes, constraints, and removals - generated Thu, 18 Apr 2024 10:51:59 -0400
-- Old definition: pgsql8/someapp_v2_composite.xml
-- New definition pgsql8/someapp_extracted_composite.xml

BEGIN;

DROP SCHEMA public CASCADE;

DROP SCHEMA search_results CASCADE;

DROP SCHEMA _p_public_sql_user CASCADE;


-- SQL STAGE STAGE3 COMMANDS


COMMIT;
