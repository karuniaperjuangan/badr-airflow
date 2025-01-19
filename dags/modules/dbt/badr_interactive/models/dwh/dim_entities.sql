SELECT 
    e.id,
    e.name,
    r.name AS regency_name,
    p.name AS province_name,
    groupArray(et.title)[1] AS tag
FROM {{ source('data_source', 'entities') }} e
LEFT JOIN {{ source('data_source', 'regencies') }} r ON e.regency_id = r.id
LEFT JOIN {{ source('data_source', 'provinces') }} p ON p.id = e.province_id
LEFT JOIN {{source('data_source', 'entity_entity_tags')}} eet ON e.id = eet.entity_id
LEFT JOIN {{source('data_source', 'entity_tags')}} et ON et.id = eet.entity_tag_id
WHERE e.deleted_at IS NULL
GROUP BY e.id, e.name, r.name, p.name