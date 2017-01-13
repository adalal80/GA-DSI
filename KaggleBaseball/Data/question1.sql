Select b.player_id, b.year, b.team_id, b.rbi, b.bb, a.g_all, a.g_3b,s.salary from batting as b
join appearances as a
on b.player_id = a.player_id
and b.year = a.year
and b.team_id = a.team_id
join salary as s
on b.player_id = s.player_id
and b.year = s.year
and b.team_id = s.team_id