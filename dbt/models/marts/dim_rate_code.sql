-- Codes and meanings straight from the TLC data dictionary.

select 1  as rate_code_id, 'Standard rate'        as rate_code_name
union all select 2,  'JFK'
union all select 3,  'Newark'
union all select 4,  'Nassau or Westchester'
union all select 5,  'Negotiated fare'
union all select 6,  'Group ride'
union all select 99, 'Unknown'
