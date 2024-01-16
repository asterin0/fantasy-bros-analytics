with qb_players as (
	SELECT player_id
		,player_name
		,player_team_id as team
		,player_position_id as pos
		,player_bye_week
		,player_owned_avg
		,player_owned_espn
		,player_owned_yahoo
		,CASE
			WHEN player_ecr_delta IS NULL
			THEN 0.0
			ELSE player_ecr_delta
			END AS player_ecr_delta
		,rank_ecr
		,rank_min
		,rank_max
		,cast(rank_ave as float) as rank_ave
		,rank_std
		,pos_rank
		,ppr_projs as ppr_projs_ros
		,half_ppr_projs as half_ppr_projs_ros
	FROM "staging"."qb"
	),
	
rb_players as (
	SELECT player_id
		,player_name
		,player_team_id as team
		,player_position_id as pos
		,player_bye_week
		,player_owned_avg
		,player_owned_espn
		,player_owned_yahoo
		,CASE
			WHEN player_ecr_delta IS NULL
			THEN 0.0
			ELSE player_ecr_delta
			END AS player_ecr_delta
		,rank_ecr
		,rank_min
		,rank_max
		,cast(rank_ave as float) as rank_ave
		,rank_std
		,pos_rank
		,ppr_projs as ppr_projs_ros
		,half_ppr_projs as half_ppr_projs_ros
	FROM "staging"."rb"
	),
	
wr_players as (
	SELECT player_id
		,player_name
		,player_team_id as team
		,player_position_id as pos
		,player_bye_week
		,player_owned_avg
		,player_owned_espn
		,player_owned_yahoo
		,CASE
			WHEN player_ecr_delta IS NULL
			THEN 0.0
			ELSE player_ecr_delta
			END AS player_ecr_delta
		,rank_ecr
		,rank_min
		,rank_max
		,cast(rank_ave as float) as rank_ave
		,rank_std
		,pos_rank
		,ppr_projs as ppr_projs_ros
		,half_ppr_projs as half_ppr_projs_ros
	FROM "staging"."wr"
	),
	
te_players as (
	SELECT player_id
		,player_name
		,player_team_id as team
		,player_position_id as pos
		,player_bye_week
		,player_owned_avg
		,player_owned_espn
		,player_owned_yahoo
		,CASE
			WHEN player_ecr_delta IS NULL
			THEN 0.0
			ELSE player_ecr_delta
			END AS player_ecr_delta
		,rank_ecr
		,rank_min
		,rank_max
		,cast(rank_ave as float) as rank_ave
		,rank_std
		,pos_rank
		,ppr_projs as ppr_projs_ros
		,half_ppr_projs as half_ppr_projs_ros
	FROM "staging"."te"
	),
	
all_players as (
	select *
	from qb_players
	union
	select *
	from rb_players
	union
	select *
	from wr_players
	union
	select *
	from te_players
	),
	
final_players as (
	select *
	from all_players
	where all_players.player_owned_avg > 5
	)

select *
from final_players