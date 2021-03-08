-- someapp_extracted_upgrade_stage1_schema1.sql
-- DBSteward stage 1 structure additions and modifications - generated Mon, 08 Mar 2021 20:11:42 +0000
-- Old definition: someapp_v2_composite.xml
-- New definition someapp_extracted_composite.xml

BEGIN;


-- SQL STAGE STAGE1BEFORE COMMANDS

DROP VIEW IF EXISTS public.group_list_view;

CREATE OR REPLACE FUNCTION _p_public_sql_user.insert_trigger() RETURNS trigger
AS $_$
  DECLARE
    	mod_result INT;
    BEGIN
    	mod_result := NEW.user_id % 4;
    	IF (mod_result = 0) THEN
    		INSERT INTO _p_public_sql_user.partition_0 VALUES (NEW.*);
    	ELSEIF (mod_result = 1) THEN
    		INSERT INTO _p_public_sql_user.partition_1 VALUES (NEW.*);
    	ELSEIF (mod_result = 2) THEN
    		INSERT INTO _p_public_sql_user.partition_2 VALUES (NEW.*);
    	ELSEIF (mod_result = 3) THEN
    		INSERT INTO _p_public_sql_user.partition_3 VALUES (NEW.*);
    	END IF;
    	RETURN NULL;
    END;
$_$
LANGUAGE plpgsql
VOLATILE;

ALTER FUNCTION _p_public_sql_user.insert_trigger() OWNER TO pgsql;

COMMENT ON FUNCTION _p_public_sql_user.insert_trigger() IS 'DBSteward auto-generated for table partition of public.sql_user';

CREATE OR REPLACE FUNCTION public.destroy_session(character varying) RETURNS void
AS $_$
  DELETE FROM session_information WHERE session_id=$1;
$_$
LANGUAGE sql
VOLATILE;

ALTER FUNCTION public.destroy_session(character varying) OWNER TO pgsql;

COMMENT ON FUNCTION public.destroy_session(character varying) IS 'Deletes session data from the database';

ALTER TABLE public.sql_user DROP CONSTRAINT sql_user_user_status_list_id_fkey;

ALTER TABLE public.sql_user DROP CONSTRAINT sql_user_user_access_level_id_fkey;

ALTER TABLE _p_public_sql_user.partition_0 DROP CONSTRAINT sql_user_p_0_chk;

ALTER TABLE _p_public_sql_user.partition_0 DROP CONSTRAINT p0_user_status_list_id_fk;

ALTER TABLE _p_public_sql_user.partition_0 DROP CONSTRAINT p0_user_access_level_id_fk;

ALTER TABLE _p_public_sql_user.partition_1 DROP CONSTRAINT sql_user_p_1_chk;

ALTER TABLE _p_public_sql_user.partition_1 DROP CONSTRAINT p1_user_status_list_id_fk;

ALTER TABLE _p_public_sql_user.partition_1 DROP CONSTRAINT p1_user_access_level_id_fk;

ALTER TABLE _p_public_sql_user.partition_2 DROP CONSTRAINT sql_user_p_2_chk;

ALTER TABLE _p_public_sql_user.partition_2 DROP CONSTRAINT p2_user_status_list_id_fk;

ALTER TABLE _p_public_sql_user.partition_2 DROP CONSTRAINT p2_user_access_level_id_fk;

ALTER TABLE _p_public_sql_user.partition_3 DROP CONSTRAINT sql_user_p_3_chk;

ALTER TABLE _p_public_sql_user.partition_3 DROP CONSTRAINT p3_user_status_list_id_fk;

ALTER TABLE _p_public_sql_user.partition_3 DROP CONSTRAINT p3_user_access_level_id_fk;

ALTER TABLE public.session_information DROP CONSTRAINT session_information_user_id_fkey;

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT sql_user_p_0_chk CHECK ((user_id % (4)::bigint) = 0);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT sql_user_p_1_chk CHECK ((user_id % (4)::bigint) = 1);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT sql_user_p_2_chk CHECK ((user_id % (4)::bigint) = 2);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT sql_user_p_3_chk CHECK ((user_id % (4)::bigint) = 3);

ALTER TABLE public.group_list
  /* changing from type bigserial */
  ALTER COLUMN group_list_id TYPE serial,
  ALTER COLUMN group_visible SET DEFAULT true;

ALTER TABLE public.user_access_level
  /* changing from type int */
  ALTER COLUMN user_access_level_id TYPE integer,
  /* changing from type varchar(10) */
  ALTER COLUMN user_access_level TYPE character varying(10);

ALTER TABLE public.user_status_list
  /* changing from type int */
  ALTER COLUMN user_status_list_id TYPE integer;

ALTER TABLE public.sql_user
  /* changing from type bigserial */
  ALTER COLUMN user_id TYPE serial,
  /* changing from type int */
  ALTER COLUMN user_status_list_id TYPE integer,
  /* changing from type int */
  ALTER COLUMN user_access_level_id TYPE integer;

