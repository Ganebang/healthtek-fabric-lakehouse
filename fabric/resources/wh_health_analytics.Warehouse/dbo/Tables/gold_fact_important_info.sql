CREATE TABLE [dbo].[gold_fact_important_info] (
    [info_key]         VARCHAR (8000) NULL,
    [drug_sk]          BIGINT         NULL,
    [time_sk]          INT            NULL,
    [time_end_sk]      INT            NULL,
    [cis_code]         VARCHAR (8000) NULL,
    [info_type]        VARCHAR (8000) NULL,
    [info_title]       VARCHAR (8000) NULL,
    [info_url]         VARCHAR (8000) NULL,
    [date_debut]       DATE           NULL,
    [date_fin]         DATE           NULL,
    [is_active]        BIT            NULL,
    [duration_days]    INT            NULL,
    [days_remaining]   INT            NULL,
    [as_of_date]       DATE           NULL,
    [_gold_created_at] DATETIME2 (6)  NULL,
    [_pipeline_run_id] VARCHAR (8000) NULL
);


GO