-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "f594b6ab-709b-4089-9f71-44f9bdb59e68",
-- META       "default_lakehouse_name": "IntellaTriageLakehouse",
-- META       "default_lakehouse_workspace_id": "6f688f9a-787a-46c6-8068-8d6e88edd051",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "f594b6ab-709b-4089-9f71-44f9bdb59e68"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE fact_nicecxone_completed_contacts_bak_20251217
USING DELTA
AS
SELECT *
FROM fact_nicecxone_completed_contacts;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
