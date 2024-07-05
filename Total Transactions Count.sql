select count(*) as total_user_count,
    sum(transaction_count) as total_transaction_count,
    sum(amount_usd) as total_bridged_amount,
    max(active_source_chain_count) as max_active_source_chain_count,
    max(active_destination_chain_count) as max_active_destination_chain_count,
    max(active_transaction_contract_count) as max_active_transaction_contract_count,
    max(active_days_count) as max_active_days_count,
    max(active_weeks_count) as max_active_weeks_count,
    max(active_months_count) as max_active_months_count,
    max(lz_age_days) as max_lz_age_days,
    max(rank_score) as max_rank_score
from query_2465489

    
