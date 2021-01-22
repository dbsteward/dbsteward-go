-- example/someapp_v2_upgrade_stage1_schema1.sql
-- DBSteward stage 1 structure additions and modifications - generated Fri, 22 Jan 2021 16:58:46 -0500
-- Old definition: example/someapp_v1_composite.xml
-- New definition example/someapp_v2_composite.xml

BEGIN;


-- SQL STAGE STAGE1BEFORE COMMANDS

DROP VIEW IF EXISTS public.group_list_view;

ALTER TABLE public.sql_user DROP CONSTRAINT user_name_unq;

ALTER TABLE _p_public_sql_user.partition_0 DROP CONSTRAINT p0_user_name_unq;

ALTER TABLE _p_public_sql_user.partition_1 DROP CONSTRAINT p1_user_name_unq;

ALTER TABLE _p_public_sql_user.partition_2 DROP CONSTRAINT p2_user_name_unq;

ALTER TABLE _p_public_sql_user.partition_3 DROP CONSTRAINT p3_user_name_unq;

CREATE TABLE public.user_access_level(
	user_access_level_id int,
	user_access_level varchar(10),
	can_see_invisible_users boolean
);

ALTER TABLE public.user_access_level
  OWNER TO pgsql;

ALTER TABLE public.user_access_level
  ALTER COLUMN user_access_level SET DEFAULT true;

ALTER TABLE public.user_access_level
  ALTER COLUMN user_access_level SET NOT NULL;

ALTER TABLE public.user_access_level
  ALTER COLUMN can_see_invisible_users SET DEFAULT false;

ALTER TABLE public.user_access_level
  ALTER COLUMN can_see_invisible_users SET NOT NULL;

ALTER TABLE public.user_access_level
  ADD CONSTRAINT user_access_level_pkey PRIMARY KEY (user_access_level_id);

ALTER TABLE public.group_list
  ADD COLUMN group_visible boolean DEFAULT TRUE,
  /* changing from type varchar(100) */
  ALTER COLUMN group_description TYPE text;

ALTER TABLE public.sql_user
  ADD COLUMN email text,
  ADD COLUMN user_access_level_id int DEFAULT 1;

-- column rename from oldColumnName specification
ALTER TABLE public.sql_user
  RENAME COLUMN somecol TO somecol1;

UPDATE public.sql_user
SET user_access_level_id = DEFAULT
WHERE user_access_level_id IS NULL;

ALTER TABLE public.sql_user
  ADD CONSTRAINT sql_user_user_access_level_id_fkey FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

ALTER TABLE _p_public_sql_user.partition_0
  ADD CONSTRAINT p0_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

ALTER TABLE _p_public_sql_user.partition_1
  ADD CONSTRAINT p1_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

ALTER TABLE _p_public_sql_user.partition_2
  ADD CONSTRAINT p2_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

ALTER TABLE _p_public_sql_user.partition_3
  ADD CONSTRAINT p3_user_access_level_id_fk FOREIGN KEY (user_access_level_id) REFERENCES public.user_access_level (user_access_level_id);

GRANT SELECT ON TABLE public.user_access_level TO someapp;

GRANT SELECT ON TABLE public.user_access_level TO someapp_readonly;


-- SQL STAGE STAGE1 COMMANDS


COMMIT;
