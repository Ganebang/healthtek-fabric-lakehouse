CREATE TABLE [dbo].[gold_fact_shortage] (
    [shortage_key]      VARCHAR (8000) NULL,
    [drug_sk]           BIGINT         NULL,
    [presentation_sk]   BIGINT         NULL,
    [status_sk]         BIGINT         NULL,
    [time_sk]           INT            NULL,
    [time_end_sk]       INT            NULL,
    [cis_code]          VARCHAR (8000) NULL,
    [cip13]             VARCHAR (8000) NULL,
    [is_cis_level]      BIT            NULL,
    [date_debut]        DATE           NULL,
    [date_fin]          DATE           NULL,
    [date_mise_a_jour]  DATE           NULL,
    [code_statut]       INT            NULL,
    [classification]    VARCHAR (8000) NULL,
    [is_active]         BIT            NULL,
    [duration_days]     INT            NULL,
    [duration_bucket]   VARCHAR (8000) NULL,
    [days_since_update] INT            NULL,
    [remise_depassee]   BIT            NULL,
    [lien_ansm]         VARCHAR (8000) NULL,
    [as_of_date]        DATE           NULL,
    [_gold_created_at]  DATETIME2 (6)  NULL,
    [_pipeline_run_id]  VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_fact_shortage]
    ADD CONSTRAINT [FK_shortage_drug] FOREIGN KEY ([drug_sk]) REFERENCES [dbo].[gold_dim_drug] ([drug_sk]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[gold_fact_shortage]
    ADD CONSTRAINT [FK_shortage_status] FOREIGN KEY ([status_sk]) REFERENCES [dbo].[gold_dim_status] ([status_sk]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[gold_fact_shortage]
    ADD CONSTRAINT [FK_shortage_time] FOREIGN KEY ([time_sk]) REFERENCES [dbo].[gold_dim_time] ([time_sk]) NOT ENFORCED;


GO