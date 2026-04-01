{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {% if target.name == 'prod' %}

        {{ custom_schema_name if custom_schema_name else default_schema }}

    {% else %}

        {{ default_schema }}{% if custom_schema_name %}_{{ custom_schema_name }}{% endif %}

    {% endif %}

{% endmacro %}