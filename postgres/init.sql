CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.hltv_metadata (
  hltv_id INTEGER PRIMARY KEY,
  team_a_name VARCHAR(40),
  team_b_name VARCHAR(40),
  competition VARCHAR(100),
  "date" DATE,
  match_url VARCHAR,
  demo_url TEXT
);

CREATE TABLE IF NOT EXISTS staging.kills (
  kill_id SERIAL PRIMARY KEY,
  match_id INTEGER,
  hltv_id INTEGER,
  x_pos INTEGER,
  y_pos INTEGER,
  weapon VARCHAR(50),
  damage SMALLINT
  CONSTRAINT fk_match
    FOREIGN KEY(match_id)
      REFERENCES matches(match_id)
);

CREATE TABLE IF NOT EXISTS staging.matches (
  match_id SERIAL PRIMARY KEY,
  team_a varchar(40),
  team_b varchar(40),
  competition varchar(100),
  "date" date,
  match_url varchar,
  demo_url text
);

CREATE TABLE IF NOT EXISTS staging.players (
  player_id smallint PRIMARY KEY,
  team_id smallint FOREIGN KEY,
)