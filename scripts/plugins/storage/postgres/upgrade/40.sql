ALTER TABLE fledge.readings ALTER COLUMN asset_code TYPE varchar(255);
ALTER TABLE fledge.omf_created_objects ALTER COLUMN asset_code TYPE varchar(255);
ALTER TABLE fledge.statistics ALTER COLUMN key TYPE varchar(255);
ALTER TABLE fledge.statistics_history ALTER COLUMN key TYPE varchar(255);
ALTER TABLE fledge.asset_tracker ALTER COLUMN asset TYPE varchar(255);


