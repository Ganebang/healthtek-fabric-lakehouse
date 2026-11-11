CREATE TABLE [dbo].[gold_dim_time] (
    [time_sk]          INT            NOT NULL,
    [date_value]       DATE           NULL,
    [year]             INT            NULL,
    [quarter]          INT            NULL,
    [month]            INT            NULL,
    [day]              INT            NULL,
    [week_of_year]     INT            NULL,
    [iso_year]         INT            NULL,
    [day_of_week]      INT            NULL,
    [day_name]         VARCHAR (8000) NULL,
    [month_name]       VARCHAR (8000) NULL,
    [year_month]       VARCHAR (8000) NULL,
    [year_quarter]     VARCHAR (8000) NULL,
    [is_weekend]       BIT            NULL,
    [is_month_start]   BIT            NULL,
    [is_month_end]     BIT            NULL,
    [is_year_start]    BIT            NULL,
    [_gold_created_at] DATETIME2 (6)  NULL,
    [_pipeline_run_id] VARCHAR (8000) NULL
);


GO

ALTER TABLE [dbo].[gold_dim_time]
    ADD CONSTRAINT [PK_gold_dim_time] PRIMARY KEY NONCLUSTERED ([time_sk] ASC) NOT ENFORCED;


GO