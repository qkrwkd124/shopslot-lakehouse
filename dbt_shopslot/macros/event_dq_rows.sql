{% macro event_dq_rows(input_model, stage) %}

select
    case
        when event_id is not null and trim(event_id) != '' then concat('event:', event_id)
        else concat_ws(':', 'kafka',
            coalesce(kafka_topic, 'unknown'),
            coalesce(cast(kafka_partition as string), 'unknown'),
            coalesce(cast(kafka_offset as string), 'unknown'))
    end as dq_key,
    event_id,
    event_type,
    '{{ stage }}' as validation_stage,
    validation_error,
    payload,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    bronze_ingested_at,
    current_timestamp() as first_detected_at,
    current_timestamp() as last_detected_at
from {{ input_model }}
where validation_error is not null

{% endmacro %}
