CREATE TABLE [dbo].[gold_dim_status] (
    [status_code]      INT            NULL,
    [status_label]     VARCHAR (8000) NULL,
    [is_active]        BIT            NULL,
    [severity]         INT            NULL,
    [status_sk]        BIGINT         NOT NULL,
    [_gold_created_at] DATETIME2 (6)  NULL,
    [_pipeline_run_id] VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_status]
    ADD CONSTRAINT [PK_gold_dim_status] PRIMARY KEY NONCLUSTERED ([status_sk] ASC) NOT ENFORCED;


GO