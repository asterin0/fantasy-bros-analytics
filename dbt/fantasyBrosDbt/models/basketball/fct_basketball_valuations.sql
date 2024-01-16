with flex_benchmark as (
	select *
	from "basketball"."dim_basketball_benchmarks"
	where "pos" = 'ALL'
	),

player_benchmarks as (
	select players."player"
		,players."team"
		,players."pos_pg"
		,players."pos_sg"
		,players."pos_sf"
		,players."pos_pf"
		,players."pos_c"
		,players."pos_g"
		,players."pos_f"
		,players."proj_fantasy_pts"
		,pg_bench."proj_benchmark_pts" as pg_benchmark_pts
		,sg_bench."proj_benchmark_pts" as sg_benchmark_pts
		,sf_bench."proj_benchmark_pts" as sf_benchmark_pts
		,pf_bench."proj_benchmark_pts" as pf_benchmark_pts
		,c_bench."proj_benchmark_pts" as c_benchmark_pts
		,g_bench."proj_benchmark_pts" as g_benchmark_pts
		,f_bench."proj_benchmark_pts" as f_benchmark_pts
		,flex_benchmark."proj_benchmark_pts" as all_pos_benchmark_pts
		,players."proj_fantasy_pts" - pg_bench."proj_benchmark_pts" as value_over_pg
		,players."proj_fantasy_pts" - sg_bench."proj_benchmark_pts" as value_over_sg
		,players."proj_fantasy_pts" - sf_bench."proj_benchmark_pts" as value_over_sf
		,players."proj_fantasy_pts" - pf_bench."proj_benchmark_pts" as value_over_pf
		,players."proj_fantasy_pts" - c_bench."proj_benchmark_pts" as value_over_c
		,players."proj_fantasy_pts" - g_bench."proj_benchmark_pts" as value_over_g
		,players."proj_fantasy_pts" - f_bench."proj_benchmark_pts" as value_over_f
		,players."proj_fantasy_pts" - flex_benchmark."proj_benchmark_pts" as value_over_all_pos
	from "basketball"."dim_basketball_players" players
	left join "basketball"."dim_basketball_benchmarks" as pg_bench on ((players."pos_pg"=1) and (pg_bench."pos"='PG'))
	left join "basketball"."dim_basketball_benchmarks" as sg_bench on ((players."pos_sg"=1) and (sg_bench."pos"='SG'))
	left join "basketball"."dim_basketball_benchmarks" as sf_bench on ((players."pos_sf"=1) and (sf_bench."pos"='SF'))
	left join "basketball"."dim_basketball_benchmarks" as pf_bench on ((players."pos_pf"=1) and (pf_bench."pos"='PF'))
	left join "basketball"."dim_basketball_benchmarks" as c_bench on ((players."pos_c"=1) and (c_bench."pos"='C'))
	left join "basketball"."dim_basketball_benchmarks" as g_bench on ((players."pos_g"=1) and (g_bench."pos"='G'))
	left join "basketball"."dim_basketball_benchmarks" as f_bench on ((players."pos_f"=1) and (f_bench."pos"='F'))
	cross join flex_benchmark
	),
	
projections_over_time as (
	SELECT history."player"
		,CASE WHEN EXISTS (SELECT history."projection_date" FROM "basketball"."dim_basketball_players_history" history WHERE history."projection_date"=(CURRENT_DATE-1))
			THEN SUM(CASE WHEN history."projection_date"=(CURRENT_DATE) THEN history."proj_fantasy_pts" ELSE 0.0 END)-SUM(CASE WHEN history."projection_date"=(CURRENT_DATE-1) THEN history."proj_fantasy_pts" ELSE 0.0 END) 
			ELSE 0.0
			END AS one_day_projection_delta
		,CASE WHEN EXISTS (SELECT history."projection_date" FROM "basketball"."dim_basketball_players_history" history WHERE history."projection_date"=(CURRENT_DATE-5))
			THEN SUM(CASE WHEN history."projection_date"=(CURRENT_DATE) THEN history."proj_fantasy_pts" ELSE 0.0 END)-SUM(CASE WHEN history."projection_date"=(CURRENT_DATE-5) THEN history."proj_fantasy_pts" ELSE 0.0 END) 
			ELSE 0.0
			END AS five_day_projection_delta
		,CASE WHEN EXISTS (SELECT history."projection_date" FROM "basketball"."dim_basketball_players_history" history WHERE history."projection_date"=(CURRENT_DATE-10))
			THEN SUM(CASE WHEN history."projection_date"=(CURRENT_DATE) THEN history."proj_fantasy_pts" ELSE 0.0 END)-SUM(CASE WHEN history."projection_date"=(CURRENT_DATE-10) THEN history."proj_fantasy_pts" ELSE 0.0 END) 
			ELSE 0.0
			END AS ten_day_projection_delta
	FROM "basketball"."dim_basketball_players_history" history
	GROUP BY history."player"
	),
	
valuations as (
	select player_benchmarks.*
		,projections_over_time.one_day_projection_delta
		,projections_over_time.five_day_projection_delta
		,projections_over_time.ten_day_projection_delta
	from player_benchmarks
	left join projections_over_time on (player_benchmarks."player"=projections_over_time."player")
	)
	
select *
from valuations