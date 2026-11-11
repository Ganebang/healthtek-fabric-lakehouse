CREATE TABLE [dbo].[gold_dim_substance] (
    [code_substance]         VARCHAR (8000) NULL,
    [denomination_substance] VARCHAR (8000) NULL,
    [drug_count]             BIGINT         NULL,
    [drug_count_as_active]   BIGINT         NULL,
    [substance_sk]           BIGINT         NOT NULL,
    [_gold_created_at]       DATETIME2 (6)  NULL,
    [_pipeline_run_id]       VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_substance]
    ADD CONSTRAINT [PK_gold_dim_substance] PRIMARY KEY NONCLUSTERED ([substance_sk] ASC) NOT ENFORCED;


GO