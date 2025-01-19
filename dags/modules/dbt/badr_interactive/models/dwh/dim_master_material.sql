SELECT 
    id,
    name,
    is_vaccine
FROM {{ source('data_source', 'master_materials') }} ms
WHERE deleted_at IS NULL