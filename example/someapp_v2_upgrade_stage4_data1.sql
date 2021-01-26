-- example/someapp_v2_upgrade_stage4_data1.sql
-- DBSteward stage 4 data definition changes and additions - generated Tue, 26 Jan 2021 13:40:35 -0500
-- Old definition: example/someapp_v1_composite.xml
-- New definition example/someapp_v2_composite.xml

BEGIN;

INSERT INTO public.user_access_level (user_access_level_id, user_access_level, can_see_invisible_users) VALUES (1, 'User', 'false');

INSERT INTO public.user_access_level (user_access_level_id, user_access_level, can_see_invisible_users) VALUES (10, 'Moderator', 'false');

INSERT INTO public.user_access_level (user_access_level_id, user_access_level, can_see_invisible_users) VALUES (50, 'Admin', 'true');


-- SQL STAGE STAGE4 COMMANDS


COMMIT;
