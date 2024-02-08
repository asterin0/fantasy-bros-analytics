with player_proj_ranked as (
	select players.pos
		,players.ppr_projs_ros
		,DENSE_RANK() over (partition by players.pos order by players.ppr_projs_ros desc) as rank_by_ppr_proj_ros
		,players.half_ppr_projs_ros
		,DENSE_RANK() over (partition by players.pos order by players.half_ppr_projs_ros desc) as rank_by_half_ppr_proj_ros
	from "football".dim_players players
	),

flex_proj_ranked as (
	select players.pos
		,players.ppr_projs_ros
		,DENSE_RANK() over (order by players.ppr_projs_ros desc) as rank_by_ppr_proj_ros
		,players.half_ppr_projs_ros
		,DENSE_RANK() over (order by players.half_ppr_projs_ros desc) as rank_by_half_ppr_proj_ros
	from "football".dim_players players
	where players.pos in ('RB', 'WR', 'TE')
	),

ppr_benchmarks as (
	select pos
		,ppr_projs_ros as ppr_benchmark_ros
	from player_proj_ranked as ppr_rk
	where (pos='QB' and rank_by_ppr_proj_ros=11)
		or (pos='RB' and rank_by_ppr_proj_ros=21)
		or (pos='WR' and rank_by_ppr_proj_ros=21)
		or (pos='TE' and rank_by_ppr_proj_ros=11)
	union
	select 'FLEX' as pos
		,ppr_projs_ros as ppr_benchmark
	from flex_proj_ranked
	where rank_by_ppr_proj_ros = 51
	),
	
half_ppr_benchmarks as (	
	select pos
		,half_ppr_projs_ros as half_ppr_benchmark_ros
	from player_proj_ranked as half_ppr_rk
	where (pos='QB' and rank_by_half_ppr_proj_ros=13)
		or (pos='RB' and rank_by_half_ppr_proj_ros=25)
		or (pos='WR' and rank_by_half_ppr_proj_ros=13)
		or (pos='TE' and rank_by_half_ppr_proj_ros=13)
	union
	select 'FLEX' as pos
		,half_ppr_projs_ros as half_ppr_benchmark
	from flex_proj_ranked
	where rank_by_half_ppr_proj_ros = 41
	),

total_benchmarks as (
	select ppr.pos
		,ppr.ppr_benchmark_ros
		,hppr.half_ppr_benchmark_ros
	from ppr_benchmarks ppr
	inner join half_ppr_benchmarks hppr on hppr.pos=ppr.pos
	)
	
select *
from total_benchmarks