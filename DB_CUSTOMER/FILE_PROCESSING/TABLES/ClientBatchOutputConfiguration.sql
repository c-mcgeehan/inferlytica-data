CREATE OR REPLACE TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION (
    APP_ORGANIZATION_ID VARCHAR NOT NULL,
    APP_BATCH_ID VARCHAR NOT NULL,
    APP_FILE_ID VARCHAR NOT NULL,
    APP_PRESET_ID VARCHAR,
    APP_PRESET_LABEL VARCHAR,
    CONFIG_JSON VARIANT NOT NULL,
    CREATED_TS TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CLIENT_BATCH_OUTPUT_CONFIGURATION
        PRIMARY KEY (APP_ORGANIZATION_ID, APP_BATCH_ID, APP_FILE_ID)
);

SELECT *
FROM  CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;

{
  "attributes": [
    {
      "code": "GNDRPR",
      "label": "Gender Predictions"
    }
  ],
  "output_fields": [
    {
      "field_name": "RECORD_ID",
      "sort_order": null
    },
    {
      "field_name": "FIRST_NAME",
      "sort_order": null
    },
    {
      "field_name": "LAST_NAME",
      "sort_order": null
    },
    {
      "field_name": "ZIP",
      "sort_order": null
    },
    {
      "field_name": "MALE_PROBABILITY",
      "sort_order": 1
    },
    {
      "field_name": "FEMALE_PROBABILITY",
      "sort_order": 2
    },
    {
      "field_name": "GENDER_CONFIDENCE_LEVEL",
      "sort_order": 3
    },
    {
      "field_name": "PREDICTED_GENDER",
      "sort_order": 4
    }
  ],
  "retain_source_fields": true
}