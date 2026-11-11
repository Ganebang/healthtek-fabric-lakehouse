CREATE TABLE [dbo].[gold_kpi_shortage_risk_score] (
    [cis_code]                 VARCHAR (8000) NULL,
    [drug_sk]                  BIGINT         NULL,
    [denomination]             VARCHAR (8000) NULL,
    [substance_active_primary] VARCHAR (8000) NULL,
    [atc_level1]               VARCHAR (8000) NULL,
    [atc_level1_label]         VARCHAR (8000) NULL,
    [is_mitm]                  BIT            NULL,
    [in_generic_group]         BIT            NULL,
    [alternatives_available]   BIGINT         NULL,
    [no_alternative]           BIT            NULL,
    [shortage_count]           BIGINT         NULL,
    [total_shortage_days]      BIGINT         NULL,
    [avg_duration_days]        FLOAT (53)     NULL,
    [max_duration_days]        INT            NULL,
    [active_shortages]         BIGINT         NULL,
    [last_shortage_start]      DATE           NULL,
    [days_since_last_shortage] INT            NULL,
    [risk_score]               FLOAT (53)     NULL,
    [risk_category]            VARCHAR (8000) NULL,
    [as_of_date]               DATE           NULL,
    [_gold_created_at]         DATETIME2 (6)  NULL,
    [_pipeline_run_id]         VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_kpi_shortage_risk_score]
    ADD CONSTRAINT [FK_risk_drug] FOREIGN KEY ([drug_sk]) REFERENCES [dbo].[gold_dim_drug] ([drug_sk]) NOT ENFORCED;


GO