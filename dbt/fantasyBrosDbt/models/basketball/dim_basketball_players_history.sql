{{
    config(
        materialized='incremental',
        unique_key=['player', 'projection_date'],
        partition_by={
            'field':'projection_date',
            'data_type':'timestamp',
            'granularity':'day'
        }
    )
}}


with players_history as (
            SELECT players.*
                ,CURRENT_DATE as projection_date
            FROM "basketball"."dim_basketball_players" players

            {% if is_incremental() %}

                union
                SELECT *
                FROM "basketball"."dim_basketball_players_history" hist
                WHERE hist."projection_date" <> CURRENT_DATE

            {% endif %}
        )

select *
from players_history