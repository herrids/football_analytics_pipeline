CREATE TABLE IF NOT EXISTS "staging_matches" (
  "match_id" int,
  "match_date" datetime,
  "home_score" int,
  "away_score" int,
  "competition_id" int,
  "season_id" int,
  "home_team_id" int,
  "home_team_name" varchar,
  "away_team_id" int,
  "away_team_name" varchar
);

CREATE TABLE IF NOT EXISTS "staging_events" (
  "event_id" varchar,
  "period" int,
  "minute" int,
  "second" int,
  "possession" int,
  "type_id" int,
  "type_name" varchar,
  "match_id" int,
  "possession_team_id" int,
  "play_pattern" varchar,
  "team_id" int,
  "player_name" varchar,
  "location" varchar,
  "duration" decimal,
  "out" boolean
);

CREATE TABLE IF NOT EXISTS "fact_events" (
  "event_id" varchar PRIMARY KEY,
  "period" int,
  "minute" int,
  "second" int,
  "possession" int,
  "type_id" int,
  "match_id" int,
  "possesion_team_id" int,
  "play_pattern" varchar,
  "team_id" int,
  "player_id" int,
  "location_x" decimal,
  "location_y" decimal,
  "duration" decimal,
  "out" boolean
);

CREATE TABLE IF NOT EXISTS "dim_matches" (
  "match_id" int PRIMARY KEY,
  "match_date" datetime,
  "home_score" int,
  "away_score" int,
  "competition_id" int,
  "season_id" int,
  "home_team_id" int,
  "away_team_id" int
);

CREATE TABLE IF NOT EXISTS "dim_teams" (
  "team_id" int PRIMARY KEY,
  "team_name" varchar
);

CREATE TABLE IF NOT EXISTS "dim_types" (
  "type_id" int PRIMARY KEY,
  "type_name" varchar
);

CREATE TABLE IF NOT EXISTS "dim_type_attributes" (
  "attribute_name" varchar PRIMARY KEY,
  "type_id" int
);

CREATE TABLE IF NOT EXISTS "fact_event_attributes" (
  "event_attribute_id" int IDENTITY(1,1) PRIMARY KEY,
  "attribute_name" varchar,
  "event_id" varchar,
  "attribute_value" varchar
);

CREATE TABLE IF NOT EXISTS "dim_players" (
  "player_id" int PRIMARY KEY,
  "player_name" varchar,
  "nationality" varchar,
  "dob" date,
  "player_positions" varchar,
  "overall_rating" int,
  "potential" int,
  "value" decimal,
  "wage" decimal,
  "work_rate" varchar,
  "weight" int,
  "height" int,
  "weak_foot" int,
  "skill_moves" int,
  "preferred_foot" varchar,
  "shooting" int,
  "power_strength" int,
  "power_stamina" int,
  "power_shot_power" int,
  "power_long_shots" int,
  "power_jumping" int,
  "physic" int,
  "passing" int,
  "pace" int,
  "movement_sprint_speed" int,
  "movement_reactions" int,
  "movement_balance" int,
  "movement_agility" int,
  "movement_acceleration" int,
  "mentality_vision" int,
  "mentality_positioning" int,
  "mentality_penalties" int,
  "mentality_interceptions" int,
  "mentality_composure" int,
  "mentality_aggression" int,
  "goalkeeping_speed" int,
  "goalkeeping_reflexes" int,
  "goalkeeping_positioning" int,
  "goalkeeping_kicking" int,
  "goalkeeping_handling" int,
  "goalkeeping_diving" int,
  "dribbling" int,
  "defending_standing_tackle" int,
  "defending_sliding_tackle" int,
  "defending_marking_awareness" int,
  "defending" int,
  "attacking_volleys" int,
  "attacking_short_passing" int,
  "attacking_heading_accuracy" int,
  "attacking_finishing" int,
  "attacking_crossing" int
);

/*
ALTER TABLE "fact_events" ADD FOREIGN KEY ("type_id") REFERENCES "dim_types" ("type_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("match_id") REFERENCES "dim_matches" ("match_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("possesion_team_id") REFERENCES "dim_teams" ("team_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("team_id") REFERENCES "dim_teams" ("team_id");

ALTER TABLE "fact_events" ADD FOREIGN KEY ("player_id") REFERENCES "dim_players" ("player_id");

ALTER TABLE "dim_matches" ADD FOREIGN KEY ("home_team_id") REFERENCES "dim_teams" ("team_id");

ALTER TABLE "dim_matches" ADD FOREIGN KEY ("away_team_id") REFERENCES "dim_teams" ("team_id");

ALTER TABLE "dim_type_attributes" ADD FOREIGN KEY ("type_id") REFERENCES "dim_types" ("type_id");

ALTER TABLE "fact_event_attributes" ADD FOREIGN KEY ("attribute_name") REFERENCES "dim_type_attributes" ("attribute_name");

ALTER TABLE "fact_event_attributes" ADD FOREIGN KEY ("event_id") REFERENCES "fact_events" ("event_id");
*/