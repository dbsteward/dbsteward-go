-- example/someapp_v2_upgrade_stage1_schema1.sql
-- DBSteward stage 1 structure additions and modifications - generated Tue, 19 Jan 2021 10:37:52 -0500
-- Old definition: example/someapp_v1_composite.xml
-- New definition example/someapp_v2_composite.xml

BEGIN;


-- SQL STAGE STAGE1BEFORE COMMANDS

DROP VIEW IF EXISTS public.group_list_view;

ALTER TABLE _p_public_sql_user.partition_0 DROP CONSTRAINT p0_user_name_unq;

ALTER TABLE _p_public_sql_user.partition_1 DROP CONSTRAINT p1_user_name_unq;

ALTER TABLE _p_public_sql_user.partition_2 DROP CONSTRAINT p2_user_name_unq;

ALTER TABLE _p_public_sql_user.partition_3 DROP CONSTRAINT p3_user_name_unq;

ALTER TABLE public.sql_user DROP CONSTRAINT user_name_unq;

ALTER TABLE public.user_status_list
  ADD CONSTRAINT user_status_list_pkey PRIMARY KEY (user_status_list_id);

ALTER TABLE public.group_list
  ADD CONSTRAINT group_list_pkey PRIMARY KEY (group_list_id);

CREATE TABLE public.user_access_level(
	user_access_level_id int,
	user_access_level varchar(10),
	can_see_invisible_users boolean
);

ALTER TABLE public.user_access_level
  OWNER TO pgsql;

ALTER TABLE public.user_access_level ALTER COLUMN user_access_level SET DEFAULT true;

ALTER TABLE public.user_access_level ALTER COLUMN user_access_level SET NOT NULL;

ALTER TABLE public.user_access_level ALTER COLUMN can_see_invisible_users SET DEFAULT false;

ALTER TABLE public.user_access_level ALTER COLUMN can_see_invisible_users SET NOT NULL;

ALTER TABLE public.user_access_level
  ADD CONSTRAINT user_access_level_pkey PRIMARY KEY (user_access_level_id);

CREATE INDEX user_name_p1 ON _p_public_sql_user.partition_1 USING btree (user_name);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT partition_1_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT sql_user_p_1_chk CHECK ((user_id % 4) = 1);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT p1_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT p1_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

CREATE INDEX user_name_p0 ON _p_public_sql_user.partition_0 USING btree (user_name);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT partition_0_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT sql_user_p_0_chk CHECK ((user_id % 4) = 0);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT p0_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT p0_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

CREATE INDEX user_name_p2 ON _p_public_sql_user.partition_2 USING btree (user_name);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT partition_2_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT sql_user_p_2_chk CHECK ((user_id % 4) = 2);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT p2_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT p2_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

CREATE INDEX user_name_p3 ON _p_public_sql_user.partition_3 USING btree (user_name);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT partition_3_pkey PRIMARY KEY (user_id);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT sql_user_p_3_chk CHECK ((user_id % 4) = 3);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT p3_user_status_list_id_fk FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT p3_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

ALTER TABLE public.sql_user
  ADD CONSTRAINT sql_user_pkey PRIMARY KEY (user_id);

ALTER TABLE public.sql_user
  ADD CONSTRAINT sql_user_user_status_list_id_fkey FOREIGN KEY (user_status_list_id) REFERENCES public.user_status_list (user_status_list_id);

ALTER TABLE public.sql_user
  ADD CONSTRAINT sql_user_user_access_level_id_fkey FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

ALTER TABLE public.session_information
  ADD CONSTRAINT session_information_pkey PRIMARY KEY (session_id);

ALTER TABLE public.session_information
  ADD CONSTRAINT session_information_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.sql_user (user_id);

GRANT SELECT ON TABLE public.user_access_level TO someapp;

GRANT SELECT ON TABLE public.user_access_level TO someapp_readonly;


-- SQL STAGE STAGE1 COMMANDS

