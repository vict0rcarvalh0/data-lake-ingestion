CREATE VIEW IF NOT EXISTS pokemon_data_view AS
SELECT
    ingestion_date,
    JSONExtractString(line_data, 'name') AS pokemon_name,
    JSONExtractString(line_data, 'abilities') AS abilities,
    JSONExtractInt(line_data, 'base_experience') AS base_experience,
    tag
FROM working_data;