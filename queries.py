### Query to get quote data
query = """
with T1 as (

select *except(rn) from (

select proposal_id, engine_request,intermediary_id, registration_number, original_timestamp,
variant_id, customer_age, to_base64(sha256(phone)) as phone_hashed, previous_policy_expired,
 previous_policy_status, address_pincode as pincode, ncb as base_cover_ncb, vehicle_make as make, vehicle_model as model, 
 experian_rank as experian_rank_final, cc, fuel_type, ex_showroom as ex_showroom_price,transmission as transmission_type,
 vehicle_age as car_age, iib_claims_count as od_claim_history_date_accounted, 
 ir as pricing_engine_ir,
 loss_cost as pricing_engine_loss_cost,
 decile_from_ir as gmb_deciles,
 
row_number() over(partition by proposal_id order by original_timestamp desc) as rn 

from `storm-wall-185017.sg_auto_pricing_prod.pricing_event`

where 1=1 
and original_timestamp >= "2023-01-01" and original_timestamp < "2024-08-01"
and product like "%car%"
and ir is not null
and engine_request is not null
and plan_id is null
and coalesce(intermediary_id, 218)=218
) where rn = 1
),

T2 as (
select * from (
 select *,
 row_number() over (partition by registration_number order by original_timestamp ) as ron from
 T1
) where ron = 1
)

select * from T2
"""
