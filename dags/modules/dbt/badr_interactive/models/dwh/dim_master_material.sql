SELECT 
    id,
    name,
    is_vaccine
FROM {{ source('data_source', 'master_materials') }} ms