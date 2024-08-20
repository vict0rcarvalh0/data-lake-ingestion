CREATE TABLE IF NOT EXISTS working_data (
	ingestion_date DateTime,
	line_data String,
	tag String
) ENGINE = MergeTree()
ORDER BY ingestion_date