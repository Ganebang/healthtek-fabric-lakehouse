CREATE TABLE [dbo].[gold_dim_atc] (
    [code_atc]         VARCHAR (8000) NULL,
    [atc_level1]       VARCHAR (8000) NULL,
    [atc_level2]       VARCHAR (8000) NULL,
    [atc_level3]       VARCHAR (8000) NULL,
    [atc_level4]       VARCHAR (8000) NULL,
    [atc_level5]       VARCHAR (8000) NULL,
    [atc_level1_label] VARCHAR (8000) NULL,
    [atc_depth]        INT            NULL,
    [atc_sk]           BIGINT         NOT NULL,
    [_gold_created_at] DATETIME2 (6)  NULL,
    [_pipeline_run_id] VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_atc]
    ADD CONSTRAINT [PK_gold_dim_atc] PRIMARY KEY NONCLUSTERED ([atc_sk] ASC) NOT ENFORCED;


GO