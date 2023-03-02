with point_guards as (
	select *
	from "staging"."pointGuards"
	),

shooting_guards as (
	select *
	from "staging"."shootingGuards"
	),

small_forwards as (
	select *
	from "staging"."smallForwards"
	),

power_forwards as (
	select *
	from "staging"."powerForwards"
	),

centers as (
	select *
	from "staging"."centers"
	),

guards as (
	select *
	from "staging"."guards"
	),

forwards as (
	select *
	from "staging"."forwards"
	),

player_projections as (
	select ap."Player" as player
		,ap."Team" as team
		,ap."PTS" as proj_pts
		,ap."REB" as proj_reb
		,ap."AST" as proj_ast
		,ap."BLK" as proj_blk
		,ap."STL" as proj_stl
		,stat."FGA" as proj_fga
		,stat."FGA" * ap."FG%" as proj_fgm
		,ap."FG%" as "proj_fg%"
		,stat."FTA" as proj_fta
		,stat."FTA" * ap."FT%" as proj_ftm
		,ap."FT%" as "proj_ft%"
		,ap."3PM" as proj_3pm
		,ap."TO" as proj_tov
		,ap."GP" as proj_gp
		,ap."MIN" as proj_mins
	from "staging"."allPlayers" ap
	inner join "staging"."proBasketballRefStats" stat on ((split_part(stat."Player", ' ', 1)=split_part(ap."Player", ' ', 1)) and (split_part(stat."Player", ' ', 2)=split_part(ap."Player", ' ', 2)))
	),

all_players as (
	select player_projections.*
		,case
			when point_guards."Player" is null
			then 0
			else 1
			end as pos_pg
		,case
			when shooting_guards."Player" is null
			then 0
			else 1
			end as pos_sg
		,case
			when small_forwards."Player" is null
			then 0
			else 1
			end as pos_sf
		,case
			when power_forwards."Player" is null
			then 0
			else 1
			end as pos_pf
		,case
			when centers."Player" is null
			then 0
			else 1
			end as pos_c
		,case
			when guards."Player" is null
			then 0
			else 1
			end as pos_g
		,case
			when forwards."Player" is null
			then 0
			else 1
			end as pos_f
	from player_projections
	left join point_guards on (player_projections."player"=point_guards."Player")
	left join shooting_guards on (player_projections."player"=shooting_guards."Player")
	left join small_forwards on (player_projections."player"=small_forwards."Player")
	left join power_forwards on (player_projections."player"=power_forwards."Player")
	left join centers on (player_projections."player"=centers."Player")
	left join guards on (player_projections."player"=guards."Player")
	left join forwards on (player_projections."player"=forwards."Player")
	),

all_players_fantasy_pts as (
	select all_players.*
		,(all_players."proj_pts" + all_players."proj_3pm" + all_players."proj_reb" + (2 * all_players."proj_ast") + (4 * all_players."proj_stl") + (4 * all_players."proj_blk") - (2 * all_players."proj_tov") + (2 * all_players."proj_fgm") - all_players."proj_fga" + all_players."proj_ftm" - all_players."proj_fta") as proj_fantasy_pts
	from all_players
	)
	
select *
from all_players_fantasy_pts
