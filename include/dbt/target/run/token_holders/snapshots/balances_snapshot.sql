
      
  
    

    create or replace table `spock-main`.`token_holders`.`balances_snapshot`
    
    

    OPTIONS()
    as (
      

    select *,
        to_hex(md5(concat(coalesce(cast(address as string), ''), '|',coalesce(cast(
    current_timestamp()
 as string), '')))) as dbt_scd_id,
        
    current_timestamp()
 as dbt_updated_at,
        
    current_timestamp()
 as dbt_valid_from,
        nullif(
    current_timestamp()
, 
    current_timestamp()
) as dbt_valid_to
    from (
        



select * from `spock-main`.`token_holders`.`balances`

    ) sbq



    );
  
  