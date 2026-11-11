CREATE TABLE [dbo].[gold_fact_has_opinion] (
    [opinion_key]      VARCHAR (8000) NULL,
    [drug_sk]          BIGINT         NULL,
    [level_sk]         BIGINT         NULL,
    [time_sk]          INT            NULL,
    [cis_code]         VARCHAR (8000) NULL,
    [code_dossier_has] VARCHAR (8000) NULL,
    [opinion_type]     VARCHAR (8000) NULL,
    [motif_evaluation] VARCHAR (8000) NULL,
    [date_avis]        DATE           NULL,
    [level_label]      VARCHAR (8000) NULL,
    [level_rank]       INT            NULL,
    [is_latest_avis]   BIT            NULL,
    [opinion_text]     VARCHAR (8000) NULL,
    [ct_url]           VARCHAR (8000) NULL,
    [_gold_created_at] DATETIME2 (6)  NULL,
    [_pipeline_run_id] VARCHAR (8000) NULL
);


GO