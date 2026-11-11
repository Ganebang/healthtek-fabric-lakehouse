CREATE TABLE [dbo].[gold_kpi_price_movement] (
    [cip13]             VARCHAR (8000)  NULL,
    [cis_code]          VARCHAR (8000)  NULL,
    [snapshot_previous] DATE            NULL,
    [snapshot_current]  DATE            NULL,
    [price_previous]    DECIMAL (10, 2) NULL,
    [price_current]     DECIMAL (10, 2) NULL,
    [price_delta]       DECIMAL (12, 2) NULL,
    [price_delta_pct]   DECIMAL (12, 2) NULL,
    [movement]          VARCHAR (8000)  NULL,
    [_gold_created_at]  DATETIME2 (6)   NULL,
    [_pipeline_run_id]  VARCHAR (8000)  NULL
);


GO