-- Tiny static dimension. Defined in code rather than seeded because it is
-- two rows that come from the TLC data dictionary, not from data.

select 1 as vendor_id, 'Creative Mobile Technologies'  as vendor_name
union all
select 2 as vendor_id, 'Curb Mobility / VeriFone'      as vendor_name
