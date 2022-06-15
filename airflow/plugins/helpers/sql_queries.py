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
            team_id,
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
        WHERE match_id in (SELECT match_id FROM staging_matches WHERE match_date::date = '{ds}')
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
            (SELECT COUNT(DISTINCT {1}) FROM {0}) < 
            (SELECT COUNT({1}) FROM {0})
    """)
