{# https://github.com/dbt-labs/dbt-audit-helper#compare_all_columns-source #}

{{
    config(
        severity = 'warn',
        tags='audit_helper'
    )
}}

{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('stg_tpch__orders'),
    b_relation=api.Relation.create(database='analytics', schema='analytics', identifier='stg_tpch__orders'),

    primary_key='order_id'
  ) 
}}
where not perfect_match