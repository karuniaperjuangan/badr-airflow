SELECT 
    SUM(s.qty) AS total_qty, 
    ehmm.entity_id AS entity_id,
    ehmm.master_material_id AS master_material_id,
    s.activity_id AS activity_id,
    s.updatedAt as date 
FROM {{ source('data_source', 'stocks') }} s
LEFT JOIN {{ source('data_source', 'entity_has_master_materials') }} ehmm ON ehmm.id = s.entity_has_material_id
WHERE 
    ehmm.entity_id IS NOT NULL 
    AND ehmm.master_material_id IS NOT NULL 
    AND ehmm.deleted_at IS NULL
GROUP BY ehmm.entity_id,
    ehmm.master_material_id,
    s.activity_id,
    s.updatedAt
