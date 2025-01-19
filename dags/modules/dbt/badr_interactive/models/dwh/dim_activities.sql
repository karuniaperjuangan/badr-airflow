SELECT 
    id,
    name
FROM {{ source('data_source', 'master_activities') }} ma