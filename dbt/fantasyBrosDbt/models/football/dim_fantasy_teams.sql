with teams as (
    SELECT owner_name
        ,fantasy_team_name
    FROM "staging"."espnLeague"
    GROUP BY fantasy_team_name, owner_name
)

select *
from teams