select cast(active_source_chain_count as varchar) || ' Chains' as segment_type,
        count(*) as user_count
from query_2465489
group by 1
