with user_summary as (
    select user_address, 
        count(*) as transaction_count,
        min(block_time) as initial_block_time,
        max(block_time) as last_block_time,
        date_diff('day', min(block_time), now()) as lz_age_days,
        count(distinct source_chain_id) as active_source_chain_count,
        count(distinct destination_chain_id) as active_destination_chain_count,
        count(distinct transaction_contract) as active_transaction_contract_count,
        count(distinct date_trunc('day', block_time)) as active_days_count,
        count(distinct date_trunc('week', block_time)) as active_weeks_count,
        count(distinct date_trunc('month', block_time)) as active_months_count,
        -- coalesce(sum(amount_usd / power(10, p.decimals) * p.price), 0) as amount_usd
        coalesce(sum(amount_usd), 0) as amount_usd
    from layerzero.send
    group by 1
)

select count(*) as total_user_count,
    sum(transaction_count) as total_transaction_count,
    sum(amount_usd) as total_bridged_amount,
    max(active_source_chain_count) as max_active_source_chain_count,
    max(active_destination_chain_count) as max_active_destination_chain_count,
    max(active_transaction_contract_count) as max_active_transaction_contract_count,
    max(active_days_count) as max_active_days_count,
    max(active_weeks_count) as max_active_weeks_count,
    max(active_months_count) as max_active_months_count,
    max(lz_age_days) as max_lz_age_days
from user_summary -- query_2465489

    
