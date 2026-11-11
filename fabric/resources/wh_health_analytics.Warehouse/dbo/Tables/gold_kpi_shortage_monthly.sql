CREATE TABLE [dbo].[gold_kpi_shortage_monthly] (
    [year_month]        VARCHAR (8000) NULL,
    [year]              INT            NULL,
    [month]             INT            NULL,
    [month_start]       DATE           NULL,
    [code_statut]       INT            NULL,
    [classification]    VARCHAR (8000) NULL,
    [shortages_started] BIGINT         NULL,
    [still_active]      BIGINT         NULL,
    [cis_level_signals] BIGINT         NULL,
    [_gold_created_at]  DATETIME2 (6)  NULL,
    [_pipeline_run_id]  VARCHAR (8000) NULL
);


GO