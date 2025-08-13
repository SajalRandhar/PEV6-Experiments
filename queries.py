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

### OD Claim count Data
query = """
WITH Register_With_CovidImpact AS (
    SELECT 
        CASE
            WHEN EXTRACT(YEAR FROM a.Ana_St_month) = 2020 AND EXTRACT(MONTH FROM a.Ana_St_month) IN (3, 4, 5, 6, 7, 8, 9) THEN 1
            WHEN EXTRACT(YEAR FROM a.Ana_St_month) = 2021 AND EXTRACT(MONTH FROM a.Ana_St_month) IN (4, 5, 6) THEN 1
            WHEN EXTRACT(YEAR FROM a.Ana_St_month) = 2022 AND EXTRACT(MONTH FROM a.Ana_St_month) IN (1) THEN 1
            WHEN EXTRACT(YEAR FROM a.Ana_St_month) = 2023 AND EXTRACT(MONTH FROM a.Ana_St_month) IN (12) AND a.city = 'Chennai' THEN 1
            ELSE 0
        END AS Ext_Impact,
        a.*
    FROM Health_IIB_DG.Motor_Policy_LR_Calc_Final_overall_car a
),
Filtered_Policies AS (
    SELECT DISTINCT
        SPLIT(policy_number, '/')[SAFE_OFFSET(0)] AS base_policy
    FROM Test.ClaimsDash
    WHERE LOWER(TotalLoss) = 'yes'
       OR (cat_name IS NOT NULL AND lower(cat_name) != 'no catcode')
),
Target_Table AS (
    SELECT 
        base_policy, renewal_flag,
        SUM(exposure) AS exposure,
        SUM(CASE 
                WHEN LOWER(Claim_Type) = 'own damage' 
                     AND lower(claim_status) IN ('paid', 'outstanding') 
                     AND Ext_Impact = 0 
                THEN 1 
                ELSE 0 
            END) AS od_claim_count,
        SUM(CASE 
        WHEN LOWER(Claim_Type) = 'own damage' 
             AND lower(claim_status) IN ('paid', 'outstanding') 
             AND Ext_Impact = 0 
        THEN amt
        ELSE 0 
      END) AS od_claim_paid

    FROM Register_With_CovidImpact
    WHERE 
        Ana_end <= '2025-07-08' ## change it to current date
        AND Product IN ('car_od', 'car_bundled', 'car_comprehensive', 'car_acko_garage')
        AND Status2 != 'CANCELLED'
        AND base_policy NOT IN (
            SELECT base_policy
            FROM Filtered_Policies
        )
    GROUP BY base_policy, renewal_flag
)
SELECT * 
FROM Target_Table
"""
