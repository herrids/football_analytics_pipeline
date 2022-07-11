class SqlQueries:
    event_table_insert = ("""
        SELECT 
            event_id,
            period,
            minute,
            second,
            possession,
            type_id,
            match_id,
            possession_team_id,
            play_pattern,
            staging_events.team_id,
            dim_players.player_id,
            CASE 
                WHEN CHARINDEX(',', location) > 0 
                THEN CAST(SUBSTRING(location, CHARINDEX('[', location)+1, CHARINDEX('.', location)) AS float) 
                ELSE null 
            END AS location_x,
            CASE
                WHEN CHARINDEX(',', location) > 0 
                THEN CAST(SUBSTRING(location, CHARINDEX(',', location)+1, CHARINDEX(',', reverse(location))-2) AS float)
                ELSE null
            END AS location_y,
            duration,
            out
        FROM staging_events
        LEFT JOIN dim_players
        ON staging_events.player_name = dim_players.player_name
        AND staging_events.team_id = dim_players.team_id
        WHERE match_id in (SELECT match_id FROM dim_matches WHERE match_date::date = '{ds}')
            """)

    player_table_insert = ("""
        SELECT
            player_id,
            player_name,
            dim_teams.team_id,
            nationality,
            dob date,
            player_positions,
            overall_rating,
            potential,
            value,
            wage,
            work_rate,
            weight,
            height,
            weak_foot,
            skill_moves,
            preferred_foot,
            shooting,
            power_strength,
            power_stamina,
            power_shot_power,
            power_long_shots,
            power_jumping,
            physic,
            passing,
            pace,
            movement_sprint_speed,
            movement_reactions,
            movement_balance,
            movement_agility,
            movement_acceleration,
            mentality_vision,
            mentality_positioning,
            mentality_penalties,
            mentality_interceptions,
            mentality_composure,
            mentality_aggression,
            goalkeeping_speed,
            goalkeeping_reflexes,
            goalkeeping_positioning,
            goalkeeping_kicking,
            goalkeeping_handling,
            goalkeeping_diving,
            dribbling,
            defending_standing_tackle,
            defending_sliding_tackle,
            defending_marking_awareness,
            defending,
            attacking_volleys,
            attacking_short_passing,
            attacking_heading_accuracy,
            attacking_finishing,
            attacking_crossing
        FROM staging_players
        LEFT JOIN dim_teams
        ON staging_players.team_name = dim_teams.team_name
    """)

    match_table_insert = ("""
        SELECT 
            match_id, 
            match_date, 
            home_score, 
            away_score, 
            competition_id, 
            season_id, 
            home_team_id, 
            away_team_id
        FROM staging_matches
    """)

    team_table_insert = ("""
        (SELECT DISTINCT
            home_team_id as team_id, 
            home_team_name as team_name
        FROM staging_matches)
        UNION
        (SELECT DISTINCT
            away_team_id, away_team_name
        FROM staging_matches)
    """)

    type_table_insert = ("""
        SELECT DISTINCT
            type_id,
            type_name
        FROM staging_events
    """)

    attribute_table_insert = ("""
        SELECT DISTINCT
            attribute_name,
            staging_events.type_id
        FROM fact_event_attributes
        LEFT JOIN staging_events
        ON staging_events.event_id = fact_event_attributes.event_id
    """)

    # Data Quality Checks
    check_empty_tables = ("""
        SELECT COUNT({1}) = 0
        FROM {0}
    """)

    check_duplicate_rows = ("""
        SELECT 
            (SELECT COUNT(DISTINCT {1}) FROM {0}) 
            < (SELECT COUNT({1}) FROM {0})
    """)
