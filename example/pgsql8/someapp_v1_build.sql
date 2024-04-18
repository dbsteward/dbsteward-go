-- pgsql8/someapp_v1_build.sql
-- full database definition file generated Thu, 18 Apr 2024 10:51:58 -0400
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

COMMENT ON COLUMN public.sql_user.import_id IS 'id from external system';

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

