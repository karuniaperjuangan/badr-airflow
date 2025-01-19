SELECT 
    id,
    name
FROM {{ source('data_source', 'master_activities') }} ma
WHERE deleted_at IS NULL