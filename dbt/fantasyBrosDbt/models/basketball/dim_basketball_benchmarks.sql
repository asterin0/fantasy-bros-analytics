with pg_bench as (
	select 'PG' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_pg" = 1
	),
	
sg_bench as (
	select 'SG' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_sg" = 1
	),
	
sf_bench as (
	select 'SF' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_sf" = 1
	),
	
pf_bench as (
	select 'PF' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_pf" = 1
	),
	
c_bench as (
	select 'C' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_c" = 1
	),
	
g_bench as (
	select 'G' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_g" = 1
	),
	
f_bench as (
	select 'F' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	where players."pos_f" = 1
	),

all_bench as (
	select 'ALL' as pos
		,players."proj_fantasy_pts" as proj_benchmark_pts
		,DENSE_RANK() over (order by players."proj_fantasy_pts" desc) as rank_by_proj
	from "basketball"."dim_basketball_players" players
	),
	
total_bench as (
	select *
	from pg_bench
	where pg_bench."rank_by_proj" = 11
	union
	select *
	from sg_bench
	where sg_bench."rank_by_proj" = 11
	union
	select *
	from sf_bench
	where sf_bench."rank_by_proj" = 11
	union
	select *
	from pf_bench
	where pf_bench."rank_by_proj" = 11
	union
	select *
	from c_bench
	where c_bench."rank_by_proj" = 11
	union
	select *
	from g_bench
	where g_bench."rank_by_proj" = 11
	union
	select *
	from f_bench
	where f_bench."rank_by_proj" = 11
	union
	select *
	from all_bench
	where all_bench."rank_by_proj" = 51
	)
	
select total_bench."pos"
	,total_bench."proj_benchmark_pts"
from total_bench