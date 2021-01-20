-- example/someapp_v1_build.sql
-- full database definition file generated Tue, 19 Jan 2021 20:20:49 -0500
BEGIN;


-- Detected LANGUAGE SQL function public.destroy_session referring to table public.session_information in the database definition
SET check_function_bodies = false;

GRANT USAGE ON SCHEMA public TO someapp;

GRANT USAGE ON SCHEMA public TO someapp_readonly;

CREATE SCHEMA search_results;

ALTER SCHEMA search_results OWNER TO pgsql;

GRANT ALL ON SCHEMA search_results TO someapp;

GRANT USAGE ON SCHEMA search_results TO someapp_readonly;

CREATE SCHEMA _p_public_sql_user;

GRANT USAGE ON SCHEMA _p_public_sql_user TO someapp;

GRANT USAGE ON SCHEMA _p_public_sql_user TO someapp_readonly;

CREATE TABLE public.sql_user(
	user_id bigserial,
	user_name character varying(40),
	password text,
	somecol text,
	import_id character varying(32),
	register_date timestamp with time zone,
	user_status_list_id int
);

COMMENT ON TABLE public.sql_user IS 'user table comment';

ALTER TABLE public.sql_user
  OWNER TO pgsql;

ALTER TABLE public.sql_user_user_id_seq
  OWNER TO pgsql;

GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE public.sql_user TO someapp;

GRANT SELECT ON TABLE public.sql_user TO someapp_readonly;

GRANT SELECT, UPDATE ON SEQUENCE public.sql_user_user_id_seq TO someapp;

GRANT SELECT ON SEQUENCE public.sql_user_user_id_seq TO someapp_readonly;

CREATE TABLE public.user_status_list(
	user_status_list_id int,
	is_visible boolean,
	can_login boolean,
	user_status character varying(40)
);

ALTER TABLE public.user_status_list
  OWNER TO pgsql;

GRANT SELECT ON TABLE public.user_status_list TO someapp;

GRANT SELECT ON TABLE public.user_status_list TO someapp_readonly;

CREATE TABLE public.session_information(
	session_id varchar(255),
	user_id bigint,
	login_time timestamp with time zone,
	logout_time timestamp with time zone,
	last_active_time timestamp with time zone,
	ip inet,
	page varchar(40),
	start_time timestamp with time zone,
	last_time timestamp with time zone,
	data text
);

COMMENT ON TABLE public.session_information IS 'Information regarding a user''s current session';

ALTER TABLE public.session_information
  OWNER TO pgsql;

GRANT ALL ON TABLE public.session_information TO someapp;

GRANT SELECT ON TABLE public.session_information TO someapp_readonly;

CREATE TABLE public.group_list(
	group_list_id bigserial,
	group_create_time timestamp with time zone,
	group_description varchar(100),
	group_name character varying(50),
	group_permission boolean,
	group_deleted boolean
);

ALTER TABLE public.group_list
  OWNER TO pgsql;

ALTER TABLE public.group_list_group_list_id_seq
  OWNER TO pgsql;

GRANT SELECT, INSERT, UPDATE ON TABLE public.group_list TO someapp;

GRANT SELECT ON TABLE public.group_list TO someapp_readonly;

GRANT SELECT, UPDATE ON SEQUENCE public.group_list_group_list_id_seq TO someapp;

GRANT SELECT ON SEQUENCE public.group_list_group_list_id_seq TO someapp_readonly;

CREATE SEQUENCE search_results.result_tables_unique_id_seq
  INCREMENT BY 1
  NO MINVALUE
  MAXVALUE 99999
  START WITH 1
  CACHE 1
  CYCLE;

ALTER SEQUENCE search_results.result_tables_unique_id_seq OWNER TO pgsql;

GRANT USAGE, SELECT, UPDATE ON SEQUENCE search_results.result_tables_unique_id_seq TO someapp;

GRANT SELECT ON SEQUENCE search_results.result_tables_unique_id_seq TO someapp_readonly;

CREATE TABLE _p_public_sql_user.partition_0()
INHERITS (public.sql_user);

ALTER TABLE _p_public_sql_user.partition_0
  OWNER TO pgsql;

CREATE INDEX user_name_p0 ON _p_public_sql_user.partition_0 USING btree (user_name);

GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE _p_public_sql_user.partition_0 TO someapp;

GRANT SELECT ON TABLE _p_public_sql_user.partition_0 TO someapp_readonly;

CREATE TABLE _p_public_sql_user.partition_1()
INHERITS (public.sql_user);

ALTER TABLE _p_public_sql_user.partition_1
  OWNER TO pgsql;

CREATE INDEX user_name_p1 ON _p_public_sql_user.partition_1 USING btree (user_name);

GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE _p_public_sql_user.partition_1 TO someapp;

GRANT SELECT ON TABLE _p_public_sql_user.partition_1 TO someapp_readonly;

CREATE TABLE _p_public_sql_user.partition_2()
INHERITS (public.sql_user);

ALTER TABLE _p_public_sql_user.partition_2
  OWNER TO pgsql;

CREATE INDEX user_name_p2 ON _p_public_sql_user.partition_2 USING btree (user_name);

GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE _p_public_sql_user.partition_2 TO someapp;

GRANT SELECT ON TABLE _p_public_sql_user.partition_2 TO someapp_readonly;

CREATE TABLE _p_public_sql_user.partition_3()
INHERITS (public.sql_user);

ALTER TABLE _p_public_sql_user.partition_3
  OWNER TO pgsql;

CREATE INDEX user_name_p3 ON _p_public_sql_user.partition_3 USING btree (user_name);

GRANT SELECT, UPDATE, DELETE, INSERT ON TABLE _p_public_sql_user.partition_3 TO someapp;

GRANT SELECT ON TABLE _p_public_sql_user.partition_3 TO someapp_readonly;

CREATE OR REPLACE FUNCTION public.destroy_session(character varying) RETURNS VOID
AS $_$
  DELETE FROM session_information WHERE session_id=$1;
$_$
LANGUAGE sql;

ALTER FUNCTION public.destroy_session(character varying) OWNER TO pgsql;

COMMENT ON FUNCTION public.destroy_session(character varying) IS 'Deletes session data from the database';

GRANT EXECUTE ON FUNCTION public.destroy_session(character varying) TO someapp;

CREATE OR REPLACE FUNCTION _p_public_sql_user.insert_trigger() RETURNS TRIGGER
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
LANGUAGE plpgsql;

ALTER FUNCTION _p_public_sql_user.insert_trigger() OWNER TO pgsql;

COMMENT ON FUNCTION _p_public_sql_user.insert_trigger() IS 'DBSteward auto-generated for table partition of public.sql_user';

GRANT EXECUTE ON FUNCTION _p_public_sql_user.insert_trigger() TO someapp;

ALTER TABLE public.user_status_list
  ALTER COLUMN is_visible SET DEFAULT true;

ALTER TABLE public.user_status_list
  ALTER COLUMN is_visible SET NOT NULL;

ALTER TABLE public.user_status_list
  ALTER COLUMN can_login SET DEFAULT true;

ALTER TABLE public.user_status_list
  ALTER COLUMN can_login SET NOT NULL;

ALTER TABLE public.user_status_list
  ALTER COLUMN user_status SET NOT NULL;

ALTER TABLE public.session_information
  ALTER COLUMN session_id SET NOT NULL;

ALTER TABLE public.group_list
  ALTER COLUMN group_create_time SET NOT NULL;

ALTER TABLE public.group_list
  ALTER COLUMN group_permission SET DEFAULT true;

ALTER TABLE public.group_list
  ALTER COLUMN group_deleted SET DEFAULT false;

ALTER TABLE public.sql_user
  ADD CONSTRAINT sql_user_pkey PRIMARY KEY (user_id);

ALTER TABLE public.user_status_list
  ADD CONSTRAINT user_status_list_pkey PRIMARY KEY (user_status_list_id);

ALTER TABLE public.session_information
  ADD CONSTRAINT session_information_pkey PRIMARY KEY (session_id);

ALTER TABLE public.group_list
  ADD CONSTRAINT group_list_pkey PRIMARY KEY (group_list_id);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT partition_0_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT partition_1_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT partition_2_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT partition_3_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT sql_user_p_0_chk CHECK ((user_id % 4) = 0);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT p0_user_name_unq UNIQUE (user_name);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT p0_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT sql_user_p_1_chk CHECK ((user_id % 4) = 1);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT p1_user_name_unq UNIQUE (user_name);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT p1_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT sql_user_p_2_chk CHECK ((user_id % 4) = 2);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT p2_user_name_unq UNIQUE (user_name);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT p2_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT sql_user_p_3_chk CHECK ((user_id % 4) = 3);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT p3_user_name_unq UNIQUE (user_name);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT p3_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE public.sql_user
  ADD CONSTRAINT user_name_unq UNIQUE (user_name);

ALTER TABLE public.sql_user
  ADD CONSTRAINT sql_user_user_status_list_id_fkey FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE public.session_information
  ADD CONSTRAINT session_information_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.sql_user (user_id);

CREATE TRIGGER public.sql_user_part_trg
  BEFORE INSERT
  ON public.sql_user
  FOR EACH ROW
  EXECUTE PROCEDURE _p_public_sql_user.insert_trigger();

CREATE OR REPLACE VIEW public.group_list_view AS
  SELECT * FROM public.group_list WHERE group_deleted = FALSE;

ALTER VIEW public.group_list_view OWNER TO pgsql;

GRANT SELECT ON TABLE public.group_list_view TO someapp;

INSERT INTO public.user_status_list (user_status_list_id, user_status, is_visible, can_login) VALUES (1, 'Active', 'true', 'true');

INSERT INTO public.user_status_list (user_status_list_id, user_status, is_visible, can_login) VALUES (2, 'Inactive', 'false', 'true');

INSERT INTO public.user_status_list (user_status_list_id, user_status, is_visible, can_login) VALUES (3, 'Closed', 'false', 'false');

INSERT INTO public.sql_user (user_id, user_name, password, user_status_list_id, import_id, register_date) VALUES (1, 'someapp_admin', '7c6a180b36896a0a8c02787eeafb0e4c', 3, 'DEFAULT_USER', (NOW()));

SELECT setval(pg_get_serial_sequence('public.sql_user', 'user_id'), MAX(user_id), true) FROM public.sql_user;


-- NON-STAGED SQL COMMANDS

COMMIT;

