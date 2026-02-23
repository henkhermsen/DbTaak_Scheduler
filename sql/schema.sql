/* RGW Scheduler - schema (basis) */

IF OBJECT_ID('dbo.RGW_TaskRun','U') IS NULL
BEGIN
    CREATE TABLE dbo.Db_TaakScheduler_TaskRun (
        RunId               BIGINT IDENTITY(1,1) PRIMARY KEY,
        TaskId              INT NOT NULL,
        StartedAt           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        FinishedAt          DATETIME2 NULL,
        Result              NVARCHAR(20) NOT NULL,     -- Running|Success|Failed|Timeout
        ExitCode            INT NULL,
        Output              NVARCHAR(MAX) NULL,
        Error               NVARCHAR(MAX) NULL
    );
END
GO

IF OBJECT_ID('dbo.Db_TaakScheduler_Task','U') IS NULL
BEGIN
    CREATE TABLE dbo.RGW_Task (
        TaskId              INT IDENTITY(1,1) PRIMARY KEY,
        TaskName            NVARCHAR(200) NOT NULL,
        TaskType            NVARCHAR(50) NOT NULL,     -- POWERSHELL | CMD | SQL
        Payload             NVARCHAR(MAX) NOT NULL,    -- pad naar script/command (voor CMD/POWERSHELL) of SQL tekst (voor SQL)
        IsEnabled           BIT NOT NULL DEFAULT 1,

        -- Scheduling:
        CronExpression      NVARCHAR(120) NULL,        -- Quartz cron (sec verplicht), bv: 0 0/5 * * * ?
        IntervalSeconds     INT NULL,                  -- alternatief voor cron
        NextRunAt           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        TimeoutSeconds      INT NULL,

        -- Retry/backoff:
        MaxRetries          INT NOT NULL DEFAULT 1,    -- extra pogingen na de eerste
        BackoffBaseSec      INT NOT NULL DEFAULT 10,
        BackoffFactor       FLOAT NOT NULL DEFAULT 2.0,

        -- Target server:
        ServerName          NVARCHAR(200) NULL,        -- NULL/leeg = mag overal; anders exacte machine name

        -- Runtime state:
        Status              NVARCHAR(20) NOT NULL DEFAULT 'Idle', -- Idle|Running|Error
        LockedBy            NVARCHAR(200) NULL,
        LockedAt            DATETIME2 NULL,
        LastRunAt           DATETIME2 NULL,
        LastResult          NVARCHAR(20) NULL,         -- Success|Failed|Timeout
        LastMessage         NVARCHAR(MAX) NULL,

        CreatedAt           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        UpdatedAt           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_RGW_TaskRun_Task')
BEGIN
    ALTER TABLE dbo.Db_TaakScheduler_TaskRun
    ADD CONSTRAINT FK_Db_TaakScheduler_TaskRun_Task
    FOREIGN KEY (TaskId) REFERENCES dbo.Db_TaakScheduler_Task(TaskId);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Db_TaakScheduler_Task_ServerName_NextRunAt')
BEGIN
    CREATE INDEX IX_Db_TaakScheduler_Task_ServerName_NextRunAt
    ON dbo.Db_TaakScheduler_Task (IsEnabled, ServerName, NextRunAt)
    INCLUDE (TaskType, CronExpression, IntervalSeconds);
END
GO

/* voorbeeld taak */
-- INSERT dbo.Db_TaakScheduler_Task (TaskName, TaskType, Payload, CronExpression, NextRunAt, TimeoutSeconds, MaxRetries, BackoffBaseSec, BackoffFactor, ServerName)
-- VALUES ('Db_TaakScheduler_inlezen', 'CMD', 'D:\RGW\Db_TaakScheduler_inlezen.cmd', '0 0/5 * * * ?', SYSUTCDATETIME(), 600, 2, 10, 2.0, 'VNLAPP001');
