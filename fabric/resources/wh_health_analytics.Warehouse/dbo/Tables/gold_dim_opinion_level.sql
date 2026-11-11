CREATE TABLE [dbo].[gold_dim_opinion_level] (
    [opinion_type]     VARCHAR (8000) NULL,
    [level_label]      VARCHAR (8000) NULL,
    [level_rank]       INT            NULL,
    [level_sk]         BIGINT         NOT NULL,
    [is_ranked]        BIT            NULL,
    [_gold_created_at] DATETIME2 (6)  NULL,
    [_pipeline_run_id] VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_opinion_level]
    ADD CONSTRAINT [PK_gold_dim_opinion_level] PRIMARY KEY NONCLUSTERED ([level_sk] ASC) NOT ENFORCED;


GO