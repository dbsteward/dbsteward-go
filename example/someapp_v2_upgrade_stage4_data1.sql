-- example/someapp_v2_upgrade_stage4_data1.sql
-- DBSteward stage 4 data definition changes and additions - generated Tue, 26 Jan 2021 13:41:19 -0500
-- Old definition: example/someapp_v1_composite.xml
-- New definition example/someapp_v2_composite.xml

BEGIN;

INSERT INTO public.user_access_level (user_access_level_id, user_access_level, can_see_invisible_users) VALUES (1, 'User', 'false');

INSERT INTO public.user_access_level (user_access_level_id, user_access_level, can_see_invisible_users) VALUES (10, 'Moderator', 'false');

INSERT INTO public.user_access_level (user_access_level_id, user_access_level, can_see_invisible_users) VALUES (50, 'Admin', 'true');

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


-- SQL STAGE STAGE4 COMMANDS


COMMIT;
