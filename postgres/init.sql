CREATE DATABASE csgo_parser_data OWNER secrets.user;

CREATE TABLE IF NOT EXISTS hltv_metadata (
  match_id smallint PRIMARY KEY,
  team_a varchar(40),
  team_b varchar(40),
  competition varchar(100),
  "date" date,
  match_url varchar,
  demo_url text
);


