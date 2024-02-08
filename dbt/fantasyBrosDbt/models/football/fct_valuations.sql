with flex_proj_benchmarks as (
	select bench.ppr_benchmark_ros as flex_ppr_benchmark_ros
		,bench.half_ppr_benchmark_ros as flex_half_ppr_benchmark_ros
	from "football"."dim_benchmarks" bench
	where bench.pos = 'FLEX'
	),
	
player_projs as (
	select players.player_id
		,players.player_name
		,players.team
		,league.fantasy_team_name
		,league.owner_name
		,players.pos
		,players.ppr_projs_ros
		,players.half_ppr_projs_ros
		,bench.ppr_benchmark_ros
		,bench.half_ppr_benchmark_ros
		,players.player_owned_avg
		,players.pos_rank
		,players.player_ecr_delta
		,players.rank_ecr
		,players.rank_min
		,players.rank_max
		,players.rank_ave
		,players.rank_std
	from "football"."dim_players" players
	left join "staging"."espnLeague" league on league.player_name = players.player_name
	left join "football"."dim_benchmarks" bench on bench.pos=players.pos
	),

ppr_projs_over_time as (
	SELECT history.player_id
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-1))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-1) THEN history.ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS ppr_one_day_projection_delta
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-3))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-3) THEN history.ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS ppr_three_day_projection_delta
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-5))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-5) THEN history.ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS ppr_five_day_projection_delta
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-10))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-10) THEN history.ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS ppr_ten_day_projection_delta
	FROM "football"."dim_players_history" history
	GROUP BY history.player_id
	),

half_ppr_projs_over_time as (
	SELECT history.player_id
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-1))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.half_ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-1) THEN history.half_ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS half_ppr_one_day_projection_delta
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-3))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.half_ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-3) THEN history.half_ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS half_ppr_three_day_projection_delta
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-5))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.half_ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-5) THEN history.half_ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS half_ppr_five_day_projection_delta
		,CASE WHEN EXISTS (SELECT history.projection_date FROM "analysis"."dim_players_history" history WHERE history.projection_date=(CURRENT_DATE-10))
			THEN SUM(CASE WHEN history.projection_date=(CURRENT_DATE) THEN history.half_ppr_projs_ros ELSE 0.0 END)-SUM(CASE WHEN history.projection_date=(CURRENT_DATE-10) THEN history.half_ppr_projs_ros ELSE 0.0 END) 
			ELSE 0.0
			END AS half_ppr_ten_day_projection_delta
	FROM "football"."dim_players_history" history
	GROUP BY history.player_id
	),
	
valuations as (	
	select player_projs.player_id
		,player_projs.player_name
		,player_projs.team
		,player_projs.pos
		,player_projs.ppr_projs_ros
		,(player_projs.ppr_projs_ros - player_projs.ppr_benchmark_ros) as ppr_vor
		,(player_projs.ppr_projs_ros - flex_proj_benchmarks.flex_ppr_benchmark_ros) as ppr_voflex
		,player_projs.half_ppr_projs_ros
		,(player_projs.half_ppr_projs_ros - player_projs.half_ppr_benchmark_ros) as half_ppr_vor
		,(player_projs.half_ppr_projs_ros - flex_proj_benchmarks.flex_half_ppr_benchmark_ros) as half_ppr_voflex
		,player_projs.player_owned_avg
		,player_projs.pos_rank
		,player_projs.player_ecr_delta
		,player_projs.rank_ecr
		,player_projs.rank_min
		,player_projs.rank_max
		,player_projs.rank_ave
		,player_projs.rank_std
		,ppr_projs_over_time.ppr_one_day_projection_delta
		,ppr_projs_over_time.ppr_three_day_projection_delta
		,ppr_projs_over_time.ppr_five_day_projection_delta
		,ppr_projs_over_time.ppr_ten_day_projection_delta
		,half_ppr_projs_over_time.half_ppr_one_day_projection_delta
		,half_ppr_projs_over_time.half_ppr_three_day_projection_delta
		,half_ppr_projs_over_time.half_ppr_five_day_projection_delta
		,half_ppr_projs_over_time.half_ppr_ten_day_projection_delta
	from player_projs
	left join ppr_projs_over_time on player_projs.player_id = ppr_projs_over_time.player_id
	left join half_ppr_projs_over_time on player_projs.player_id = half_ppr_projs_over_time.player_id
	cross join flex_proj_benchmarks
	)
	
select *
from valuations