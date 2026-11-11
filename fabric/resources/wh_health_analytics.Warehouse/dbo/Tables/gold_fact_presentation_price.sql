CREATE TABLE [dbo].[gold_fact_presentation_price] (
    [cip13]                   VARCHAR (8000)  NULL,
    [snapshot_date]           DATE            NULL,
    [drug_sk]                 BIGINT          NULL,
    [presentation_sk]         BIGINT          NULL,
    [time_sk]                 INT             NULL,
    [cis_code]                VARCHAR (8000)  NULL,
    [prix_hors_honoraires]    DECIMAL (10, 2) NULL,
    [prix_avec_honoraires]    DECIMAL (10, 2) NULL,
    [taux_remboursement]      FLOAT (53)      NULL,
    [honoraires_dispensation] DECIMAL (10, 2) NULL,
    [has_price]               BIT             NULL,
    [is_reimbursed]           BIT             NULL,
    [_gold_created_at]        DATETIME2 (6)   NULL,
    [_pipeline_run_id]        VARCHAR (8000)  NULL
);


GO

ALTER TABLE [dbo].[gold_fact_presentation_price]
    ADD CONSTRAINT [FK_price_drug] FOREIGN KEY ([drug_sk]) REFERENCES [dbo].[gold_dim_drug] ([drug_sk]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[gold_fact_presentation_price]
    ADD CONSTRAINT [FK_price_presentation] FOREIGN KEY ([presentation_sk]) REFERENCES [dbo].[gold_dim_presentation] ([presentation_sk]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[gold_fact_presentation_price]
    ADD CONSTRAINT [FK_price_time] FOREIGN KEY ([time_sk]) REFERENCES [dbo].[gold_dim_time] ([time_sk]) NOT ENFORCED;


GO