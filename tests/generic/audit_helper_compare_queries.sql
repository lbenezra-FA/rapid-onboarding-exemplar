{% test audit_helper_compare_queries(model, table_b, primary_key, except_columns=none) %}
{{config(post_hook= "{{centralize_test_failures(results)}}", store_failures = true)}}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=model, except=exclude_columns) %}

{% set column_selection %}

  {% for column_name in column_names %} 
    {{ adapter.quote(column_name) }}{% if not loop.last %},{% endif %} 
  {% endfor %}

{% endset %}

with a as (
    select 
       {{ column_selection }}
    from {{ model }}
), 

b as (
    select 
        {{ column_selection }}
    from {{ table_b }}
),

a_except_b as (
    select * from a
    except
    select * from b
),

b_except_a as (
    select * from b
    except
    select * from a
),

union_all as (
    select 
    *
    from a_except_b
    union all 
    select 
    *
    from b_except_a
)

select * from union_all
order by {{ primary_key }}

{%endtest%}