{% macro uuid_surrogate_key(field_list) %}
    REGEXP_REPLACE(
        {{ dbt_utils.generate_surrogate_key(field_list) }},
        '^([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})$',
        '$1-$2-$3-$4-$5'
    )
{% endmacro %}
