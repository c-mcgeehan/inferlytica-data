CREATE OR REPLACE TRANSIENT TABLE CUSTOMER.MANAGEMENT.CREDIT_APPROVAL_QUEUE (
    
    -- Identifiers
    APP_ORGANIZATION_ID   VARCHAR NOT NULL,
    APP_BATCH_ID          VARCHAR NOT NULL,   -- external/app-facing batch id
    BATCH_ID              NUMBER(32,0) NOT NULL,   -- internal Snowflake batch id (if different)

    -- Status
    STATUS                VARCHAR NOT NULL DEFAULT 'PENDING',  
        -- Allowed values:
        -- PENDING   = waiting for Supabase decision
        -- APPROVED  = authorized to proceed
        -- DENIED    = insufficient credits / blocked

    -- Optional context (very useful)
    REQUESTED_CREDITS     NUMBER(18,0),       -- billable row count from Snowflake
    APPROVED_CREDITS      NUMBER(18,0),       -- what was actually reserved (usually same as requested)
    DENIAL_REASON         VARCHAR,            -- e.g. "INSUFFICIENT_CREDITS"

    -- Timestamps
    DECISION_TS           TIMESTAMP_NTZ,      -- when approved/denied

    -- Audit
    CREATED_TS            TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_TS            TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT PK_CREDIT_APPROVAL_QUEUE
        PRIMARY KEY (APP_ORGANIZATION_ID, APP_BATCH_ID)
);