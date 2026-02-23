// Program.cs (VOLLEDIG AANGEPAST)
//
// ✅ Inbegrepen (zoals jouw basis):
// 1) EventLog provider best-effort + alleen op Windows
// 2) SafeLog wrappers voorkomen logger-crash
// 3) AllowedRoots non-nullable
// 4) Duidelijke startup logging + config/DB checks
//
// ✅ EXTRA (blijft):
// 5) RunProcessAsync timeout-proof
// 6) DbTaak_Scheduler_TaskRun.ServerName vullen (Environment.MachineName)
// 7) Server-level enable/disable via dbo.DbTaak_Scheduler_Task_services (IsEnabled + ServerName)
// 8) Null-safe DB reader (voorkomt SqlNullValueException)
//
// ✅ NIEUW (payload-alive check / auto-close):
// 9) Slaat PayloadPid + PayloadStartedAt op in dbo.DbTaak_Scheduler_TaskRun
// 10) Watchdog: Running + LockedAt ouder dan N min => check PID alive; zo niet => sluit af als "Success"
//
// ✅ NIEUW (Scheduler errors naar dbo.DbTaak_Scheduler_TaskRun.Error):
// 11) System task: SYSTEM_DbTaak_Scheduler_SCHEDULER_ERRORS (TaskType=SYSTEM, IsEnabled=0) => alle scheduler errors komen als DbTaak_Scheduler_TaskRun-rij in Error kolom
//
// ⚠️ SQL 1x uitvoeren:
//   -- System task (eenmalig)
//   -- (maak aan met jouw insert script)
//   -- DbTaak_Scheduler_TaskRun kolommen bestaan al bij jou (PayloadPid/PayloadStartedAt zijn al aanwezig)
//
// NuGet:
//   dotnet add package Microsoft.Data.SqlClient
//   dotnet add package Microsoft.Extensions.Logging.EventLog
//   dotnet add package Quartz
//   dotnet add package System.Threading.AccessControl

using System.Data;
using System.Diagnostics;
using System.Runtime.Versioning;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;

var builder = Host.CreateDefaultBuilder(args)
    .UseWindowsService(options =>
    {
        options.ServiceName = "DbTaak_Scheduler";
    })
    .ConfigureLogging((ctx, logging) =>
    {
        logging.ClearProviders();

        if (Environment.UserInteractive)
            logging.AddConsole();

        if (!Environment.UserInteractive && OperatingSystem.IsWindows())
        {
            TryAddEventLogProvider(logging, sourceName: "DbTaak_Scheduler", logName: "Application");
        }

        logging.SetMinimumLevel(LogLevel.Information);
    })
    .ConfigureServices((ctx, services) =>
    {
        services.AddSingleton<AppConfig>(sp =>
        {
            var log = sp.GetRequiredService<ILoggerFactory>().CreateLogger("Config");

            var exeDir = AppContext.BaseDirectory;
            var configPath = Path.Combine(exeDir, "DbTaak_Scheduler.config");

            SafeLogHelper.Self("Config", $"BaseDirectory={exeDir}");
            SafeLogHelper.Self("Config", $"ConfigPath={configPath}");

            log.LogInformation("BaseDirectory={ExeDir}", exeDir);
            log.LogInformation("ConfigPath={ConfigPath}", configPath);

            if (!File.Exists(configPath))
                throw new FileNotFoundException($"Config ontbreekt: {configPath}");

            var cfg = JsonSerializer.Deserialize<AppConfig>(
                File.ReadAllText(configPath),
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true }
            ) ?? throw new Exception("Kon DbTaak_Scheduler.config niet parsen.");

            cfg.AllowedRoots = (cfg.AllowedRoots ?? new List<string>())
                .Select(r => r.Trim().TrimEnd('\\', '/') + "\\")
                .ToList();

            log.LogInformation("Config geladen. SqlServer={SqlServer} Db={Db} Poll={Poll}s Workers={Workers} AllowedRoots={Roots}",
                cfg.SqlServer, cfg.Database, cfg.PollSeconds, cfg.WorkerCount, string.Join(";", cfg.AllowedRoots));

            SafeLogHelper.Self("Config",
                $"Loaded. SqlServer={cfg.SqlServer} Db={cfg.Database} Poll={cfg.PollSeconds}s Workers={cfg.WorkerCount} Roots={string.Join(";", cfg.AllowedRoots)}");

            return cfg;
        });

        services.AddSingleton<DbClient>();
        services.AddHostedService<SchedulerWorker>();
    });

IHost host;

try
{
    host = builder.Build();
}
catch (Exception ex)
{
    SafeLogHelper.Self("Startup", "Host build faalde: " + ex);

    try
    {
        if (OperatingSystem.IsWindows())
            SafeLogHelper.TryWriteEventLog("DbTaak_Scheduler", "Application", "Host build faalde:\n" + ex);
    }
    catch { }

    throw;
}

await host.RunAsync();


// ==============================
// Logging helpers (anti-crash)
// ==============================

[SupportedOSPlatform("windows")]
static void TryAddEventLogProvider(ILoggingBuilder logging, string sourceName, string logName)
{
    try
    {
        logging.AddEventLog(o =>
        {
            o.SourceName = sourceName;
            o.LogName = logName;
        });

        SafeLogHelper.Self("Logging", $"EventLog provider added. Source={sourceName} Log={logName}");
    }
    catch (Exception ex)
    {
        SafeLogHelper.Self("Logging", "EventLog provider kon niet worden toegevoegd: " + ex.Message);
    }
}

static class SafeLogHelper
{
    private static readonly object _lock = new();
    private static string SelfLogPath => Path.Combine(AppContext.BaseDirectory, "DbTaak_Scheduler_selflog.txt");

    public static void Self(string area, string message)
    {
        try
        {
            lock (_lock)
            {
                File.AppendAllText(SelfLogPath,
                    $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{area}] {message}{Environment.NewLine}");
            }
        }
        catch { }
    }

    [SupportedOSPlatform("windows")]
    public static void TryWriteEventLog(string source, string logName, string message)
    {
        if (!OperatingSystem.IsWindows())
        {
            Self("EventLog", "TryWriteEventLog aangeroepen op niet-Windows; overgeslagen.");
            return;
        }

        try
        {
            if (!EventLog.SourceExists(source))
            {
                EventLog.CreateEventSource(source, logName);
            }

            EventLog.WriteEntry(source, message, EventLogEntryType.Error, 1000);
        }
        catch (Exception ex)
        {
            Self("EventLog", "WriteEntry faalde: " + ex.Message);
        }
    }
}


// ==============================
// Worker
// ==============================

sealed class SchedulerWorker : BackgroundService
{
    private const string SchedulerErrorTaskName = "SYSTEM_DbTaak_Scheduler_ERRORS";

    private readonly ILogger<SchedulerWorker> _log;
    private readonly AppConfig _cfg;
    private readonly DbClient _db;
    private readonly SemaphoreSlim _sema;
    private readonly string _instanceId;
    private readonly string _serverName;

    private DateTime _lastWatchdogUtc = DateTime.MinValue;

    // ✅ System task id (voor scheduler-errors naar DbTaak_Scheduler_TaskRun)
    private int? _schedulerErrorTaskId;

    public SchedulerWorker(ILogger<SchedulerWorker> log, AppConfig cfg, DbClient db)
    {
        _log = log;
        _cfg = cfg;
        _db = db;

        _sema = new SemaphoreSlim(Math.Max(1, _cfg.WorkerCount));
        _serverName = Environment.MachineName;
        _instanceId = $"{_serverName}:{Process.GetCurrentProcess().Id}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        SafeLogHelper.Self("Worker", $"Start. Instance={_instanceId} Poll={_cfg.PollSeconds}s Workers={_cfg.WorkerCount}");

        SafeLogInfo("DbTaak_Scheduler Scheduler gestart. Instance={Instance}. Poll={Poll}s Workers={Workers} DB={Server}/{Db} UserInteractive={UI}",
            _instanceId, _cfg.PollSeconds, _cfg.WorkerCount, _cfg.SqlServer, _cfg.Database, Environment.UserInteractive);

        try
        {
            await _db.PingAsync(stoppingToken);
            SafeLogInfo("DB connectie OK.");
        }
        catch (Exception ex)
        {
            SafeLogError(ex, "DB connectie faalde bij start. Service blijft draaien maar zal blijven falen in poll-loop.");
            await TryWriteSchedulerErrorToDbAsync("startup-db-ping", ex, stoppingToken);
        }

        // ✅ laad system task id voor scheduler-errors (best-effort)
        try
        {
            _schedulerErrorTaskId = await _db.GetTaskIdByNameAsync(SchedulerErrorTaskName, stoppingToken);
            SafeLogInfo("Scheduler error TaskId = {Id} ({Name})", _schedulerErrorTaskId, SchedulerErrorTaskName);
        }
        catch (Exception ex)
        {
            SafeLogError(ex, "Kon scheduler error TaskId niet ophalen (TaskName={Name}).", SchedulerErrorTaskName);
            // Best-effort: proberen toch te loggen naar selflog
            SafeLogHelper.Self("DbErrorLog", $"GetTaskIdByNameAsync faalde: {ex}");
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // ✅ Watchdog: check "Running" taken waarvan payload proces niet meer bestaat
                await MaybeRunWatchdogAsync(stoppingToken);

                // ✅ Server-level switch via dbo.DbTaak_Scheduler_Task_services
                var enabled = await _db.IsServiceEnabledForServerAsync(_serverName, stoppingToken);

                if (!enabled)
                {
                    SafeLogWarn("Server '{Server}' is DISABLED via dbo.DbTaak_Scheduler_Task_services (IsEnabled=0). Taken worden overgeslagen.",
                        _serverName);

                    await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, _cfg.PollSeconds)), stoppingToken);
                    continue;
                }

                var due = await _db.FetchAndLockDueTasksAsync(_cfg, _instanceId, stoppingToken);

                foreach (var task in due)
                {
                    await _sema.WaitAsync(stoppingToken);

                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await RunOneTaskAsync(task, stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            SafeLogError(ex, "Onverwachte fout bij TaskId={TaskId}", task.TaskId);
                            await TryWriteSchedulerErrorToDbAsync($"task-wrapper TaskId={task.TaskId}", ex, stoppingToken);
                        }
                        finally
                        {
                            _sema.Release();
                        }
                    }, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                SafeLogError(ex, "Fout in poll-loop");
                await TryWriteSchedulerErrorToDbAsync("poll-loop", ex, stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, _cfg.PollSeconds)), stoppingToken);
        }
    }

    private async Task TryWriteSchedulerErrorToDbAsync(string area, Exception ex, CancellationToken ct)
    {
        try
        {
            if (!_schedulerErrorTaskId.HasValue)
                return;

            // als we shutdownen, wil je meestal alsnog loggen -> best-effort zonder cancellation
            var useCt = (ct.CanBeCanceled && ct.IsCancellationRequested) ? CancellationToken.None : ct;

            await _db.InsertSchedulerErrorRunAsync(
                systemTaskId: _schedulerErrorTaskId.Value,
                serverName: _serverName,
                instanceId: _instanceId,
                area: area,
                ex: ex,
                ct: useCt
            );
        }
        catch (Exception dbEx)
        {
            SafeLogHelper.Self("DbErrorLog", $"InsertSchedulerErrorRunAsync faalde: {dbEx}");
        }
    }

    private async Task MaybeRunWatchdogAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        if ((now - _lastWatchdogUtc) < TimeSpan.FromSeconds(Math.Max(30, _cfg.WatchdogIntervalSeconds)))
            return;

        _lastWatchdogUtc = now;

        try
        {
            await RecoverStaleRunningIfPayloadStoppedAsync(ct);
        }
        catch (Exception ex)
        {
            SafeLogError(ex, "Watchdog faalde (non-fatal).");
            await TryWriteSchedulerErrorToDbAsync("watchdog", ex, ct);
        }
    }

    private async Task RecoverStaleRunningIfPayloadStoppedAsync(CancellationToken ct)
    {
        var staleMinutes = Math.Max(1, _cfg.WatchdogStaleMinutes);

        var stale = await _db.FetchStaleRunningWithPidAsync(staleMinutes, _serverName, ct);

        foreach (var x in stale)
        {
            var type = (x.TaskType ?? "").Trim().ToUpperInvariant();
            if (type is not ("CMD" or "POWERSHELL"))
                continue;

            if (!x.Pid.HasValue)
                continue;

            if (IsPidAlive(x.Pid.Value))
                continue;

            SafeLogWarn("STUCK-RECOVERY TaskId={TaskId} ({Name}) PID={Pid} niet actief. Afsluiten als Success en unlocken.",
                x.TaskId, x.TaskName, x.Pid.Value);

            var t = await _db.ReadTaskByIdAsync(x.TaskId, ct);
            var nextRunUtc = ComputeNextRunUtc(t);

            var recovered = new ExecResult(
                Success: true,
                TimedOut: false,
                ExitCode: 0,
                Pid: x.Pid,
                Output: $"Recovered: PID {x.Pid} not running; auto-closed as Success (watchdog).",
                Error: ""
            );

            await _db.FinishRunAndUnlockAsync(t, x.RunId, recovered, _instanceId, nextRunUtc, _serverName, ct);
        }
    }

    private static bool IsPidAlive(int pid)
    {
        try
        {
            var p = Process.GetProcessById(pid);
            return !p.HasExited;
        }
        catch
        {
            return false;
        }
    }

    private async Task RunOneTaskAsync(TaskRow t, CancellationToken ct)
    {
        var timeout = TimeSpan.FromSeconds(t.TimeoutSeconds ?? _cfg.DefaultTimeoutSeconds);

        SafeLogInfo("Start TaskId={TaskId} {Name} Type={Type} Timeout={Timeout}s TargetServer={Target}",
            t.TaskId, t.TaskName, t.TaskType, timeout.TotalSeconds, t.ServerName ?? "(any)");

        var runServer = _serverName;
        long runId = await _db.InsertRunAsync(t.TaskId, runServer, ct);

        ExecResult final;

        try
        {
            ValidateServerOrThrow(t);
            ValidateAllowlistOrThrow(_cfg, t);

            var maxRetries = Math.Max(0, t.MaxRetries);
            var backoffBase = Math.Max(0, t.BackoffBaseSec);
            var backoffFactor = t.BackoffFactor <= 1.0 ? 2.0 : t.BackoffFactor;

            final = new ExecResult(false, false, null, null, "", "Nog niet uitgevoerd");

            for (int attempt = 0; attempt <= maxRetries; attempt++)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    final = await ExecuteByTypeAsync(_db.ConnectionString, _db, runId, t, timeout, ct);

                    if (final.Success || final.TimedOut)
                        break;

                    if (attempt < maxRetries)
                    {
                        var delay = ComputeBackoff(backoffBase, backoffFactor, attempt);
                        SafeLogWarn("TaskId={TaskId} poging {Attempt}/{Max} mislukt, backoff {Delay}s",
                            t.TaskId, attempt + 1, maxRetries + 1, delay.TotalSeconds);
                        await Task.Delay(delay, ct);
                    }
                }
                catch (Exception ex)
                {
                    final = new ExecResult(false, false, null, null, "", ex.ToString());

                    if (attempt < maxRetries)
                    {
                        var delay = ComputeBackoff(backoffBase, backoffFactor, attempt);
                        SafeLogWarn("TaskId={TaskId} uitzondering op poging {Attempt}/{Max}, backoff {Delay}s: {Msg}",
                            t.TaskId, attempt + 1, maxRetries + 1, delay.TotalSeconds, ex.Message);
                        await Task.Delay(delay, ct);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            final = new ExecResult(false, false, null, null, "", ex.ToString());
        }

        var nextRunUtc = ComputeNextRunUtc(t);

        await _db.FinishRunAndUnlockAsync(t, runId, final, _instanceId, nextRunUtc, runServer, ct);

        var res = final.TimedOut ? "Timeout" : final.Success ? "Success" : "Failed";
        SafeLogInfo("Einde TaskId={TaskId} -> {Result}. NextRunAt(UTC)={Next}",
            t.TaskId, res, nextRunUtc.ToString("yyyy-MM-dd HH:mm:ss"));
    }

    private static void ValidateServerOrThrow(TaskRow t)
    {
        if (!string.IsNullOrWhiteSpace(t.ServerName))
        {
            var target = t.ServerName.Trim();
            var me = Environment.MachineName;

            if (!target.Equals(me, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Taak is bedoeld voor server '{target}', maar draait op '{me}'.");
        }
    }

    private static TimeSpan ComputeBackoff(int baseSec, double factor, int attemptZeroBased)
    {
        var sec = baseSec * Math.Pow(factor, attemptZeroBased);
        sec = Math.Min(sec, 15 * 60);
        return TimeSpan.FromSeconds(sec);
    }

    private static void ValidateAllowlistOrThrow(AppConfig cfg, TaskRow t)
    {
        var type = (t.TaskType ?? "").Trim().ToUpperInvariant();

        if (type is "CMD" or "POWERSHELL")
        {
            var path = (t.Payload ?? "").Trim().Trim('"');

            if (!Path.IsPathRooted(path))
                throw new InvalidOperationException($"Payload moet een absoluut pad zijn. TaskId={t.TaskId}");

            var full = Path.GetFullPath(path);

            var okRoot = (cfg.AllowedRoots ?? new List<string>()).Any(root =>
                full.StartsWith(root, StringComparison.OrdinalIgnoreCase));

            if (!okRoot)
                throw new InvalidOperationException($"Payload valt buiten allowlist roots. TaskId={t.TaskId} Path={full}");

            if (!File.Exists(full))
                throw new FileNotFoundException($"Bestand bestaat niet. TaskId={t.TaskId} Path={full}");

            var ext = Path.GetExtension(full).ToLowerInvariant();

            if (type == "POWERSHELL" && ext != ".ps1")
                throw new InvalidOperationException($"Alleen .ps1 toegestaan voor POWERSHELL. TaskId={t.TaskId}");

            if (type == "CMD" && ext is not (".cmd" or ".bat" or ".exe"))
                throw new InvalidOperationException($"Alleen .cmd/.bat/.exe toegestaan voor CMD. TaskId={t.TaskId}");
        }
    }

    private static async Task<ExecResult> ExecuteByTypeAsync(string cs, DbClient db, long runId, TaskRow t, TimeSpan timeout, CancellationToken ct)
    {
        var type = (t.TaskType ?? "").Trim().ToUpperInvariant();

        return type switch
        {
            "CMD" => await RunProcessAsync(
                "cmd.exe",
                $"/c \"{(t.Payload ?? "").Trim().Trim('\"')}\"",
                timeout,
                ct,
                onStarted: async (pid) =>
                {
                    await db.SetRunPayloadProcessAsync(runId, pid, DateTime.UtcNow, ct);
                }),

            "POWERSHELL" => await RunProcessAsync(
                "powershell.exe",
                $"-NoProfile -ExecutionPolicy Bypass -File \"{(t.Payload ?? "").Trim().Trim('\"')}\"",
                timeout,
                ct,
                onStarted: async (pid) =>
                {
                    await db.SetRunPayloadProcessAsync(runId, pid, DateTime.UtcNow, ct);
                }),

            "SQL" => await RunSqlScalarAsync(cs, t.Payload ?? "", timeout, ct),

            _ => new ExecResult(false, false, null, null, "", $"Onbekend TaskType: {t.TaskType}")
        };
    }

    private static async Task<ExecResult> RunSqlScalarAsync(string cs, string sql, TimeSpan timeout, CancellationToken ct)
    {
        using var conn = new SqlConnection(cs);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = (int)Math.Ceiling(timeout.TotalSeconds);

        await conn.OpenAsync(ct);
        var obj = await cmd.ExecuteScalarAsync(ct);
        return new ExecResult(true, false, 0, null, obj?.ToString() ?? "(null)", "");
    }

    private static async Task<ExecResult> RunProcessAsync(
        string file,
        string args,
        TimeSpan timeout,
        CancellationToken ct,
        Func<int, Task>? onStarted = null)
    {
        using var p = new Process();
        p.StartInfo = new ProcessStartInfo
        {
            FileName = file,
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        p.Start();
        var pid = p.Id;

        if (onStarted != null)
        {
            try { await onStarted(pid); } catch { }
        }

        var stdoutTask = p.StandardOutput.ReadToEndAsync();
        var stderrTask = p.StandardError.ReadToEndAsync();

        var waitTask = p.WaitForExitAsync(ct);
        var timeoutTask = Task.Delay(timeout, ct);

        var finished = await Task.WhenAny(waitTask, timeoutTask);

        if (finished != waitTask)
        {
            try { p.Kill(true); } catch { }

            string stdout = "";
            string stderr = "";
            try { stdout = await stdoutTask; } catch { }
            try { stderr = await stderrTask; } catch { }

            return new ExecResult(false, true, null, pid, stdout, stderr);
        }

        var stdoutOk = await stdoutTask;
        var stderrOk = await stderrTask;

        return new ExecResult(p.ExitCode == 0, false, p.ExitCode, pid, stdoutOk, stderrOk);
    }

    private static DateTime ComputeNextRunUtc(TaskRow t)
    {
        var nowUtc = DateTime.UtcNow;

        if (!string.IsNullOrWhiteSpace(t.CronExpression))
        {
            var cron = new CronExpression(t.CronExpression.Trim());
            var next = cron.GetNextValidTimeAfter(new DateTimeOffset(nowUtc));
            if (next.HasValue) return next.Value.UtcDateTime;
            return nowUtc.AddHours(24);
        }

        if (t.IntervalSeconds.HasValue && t.IntervalSeconds.Value > 0)
            return nowUtc.AddSeconds(t.IntervalSeconds.Value);

        return nowUtc.AddHours(24);
    }

    private void SafeLogInfo(string message, params object[] args)
    {
        try { _log.LogInformation(message, args); }
        catch (Exception ex)
        {
            SafeLogHelper.Self("Logger", "LogInformation faalde: " + ex.Message + " | msg=" + message);
        }
    }

    private void SafeLogWarn(string message, params object[] args)
    {
        try { _log.LogWarning(message, args); }
        catch (Exception ex)
        {
            SafeLogHelper.Self("Logger", "LogWarning faalde: " + ex.Message + " | msg=" + message);
        }
    }

    private void SafeLogError(Exception ex, string message, params object[] args)
    {
        try { _log.LogError(ex, message, args); }
        catch (Exception lx)
        {
            SafeLogHelper.Self("Logger", "LogError faalde: " + lx.Message + " | original=" + ex);
        }
    }
}


// ==============================
// DB client
// ==============================

sealed class DbClient
{
    public string ConnectionString { get; }

    public DbClient(AppConfig cfg)
    {
        var b = new SqlConnectionStringBuilder
        {
            DataSource = cfg.SqlServer,
            InitialCatalog = cfg.Database,
            Encrypt = true,
            TrustServerCertificate = true,
            IntegratedSecurity = cfg.UseIntegratedSecurity
        };

        if (!cfg.UseIntegratedSecurity)
        {
            b.UserID = cfg.SqlUser ?? "";
            b.Password = cfg.SqlPassword ?? "";
        }

        ConnectionString = b.ConnectionString;
    }

    public async Task PingAsync(CancellationToken ct)
    {
        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1;";
        await conn.OpenAsync(ct);
        _ = await cmd.ExecuteScalarAsync(ct);
    }

    public async Task<int> GetTaskIdByNameAsync(string taskName, CancellationToken ct)
    {
        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT TOP(1) TaskId
FROM dbo.DbTaak_Scheduler_Task
WHERE TaskName = @TaskName;";
        cmd.Parameters.Add(new SqlParameter("@TaskName", SqlDbType.NVarChar, 200) { Value = taskName });

        await conn.OpenAsync(ct);
        var obj = await cmd.ExecuteScalarAsync(ct);
        if (obj == null || obj == DBNull.Value)
            throw new Exception($"TaskName '{taskName}' niet gevonden in dbo.DbTaak_Scheduler_Task.");
        return Convert.ToInt32(obj);
    }

    // ✅ Scheduler errors => dbo.DbTaak_Scheduler_TaskRun.Error (als losse TaskRun rij)
    public async Task InsertSchedulerErrorRunAsync(
        int systemTaskId,
        string serverName,
        string instanceId,
        string area,
        Exception ex,
        CancellationToken ct)
    {
        var err = $"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}Z] Area={area} Instance={instanceId}\n{ex}";

        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT dbo.DbTaak_Scheduler_TaskRun (TaskId, Result, FinishedAt, Error, ServerName)
VALUES (@TaskId, 'Error', SYSUTCDATETIME(), @Error, @ServerName);";

        cmd.Parameters.Add(new SqlParameter("@TaskId", SqlDbType.Int) { Value = systemTaskId });
        cmd.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.NVarChar, 200) { Value = serverName });
        cmd.Parameters.Add(new SqlParameter("@Error", SqlDbType.NVarChar, -1) { Value = (object)err ?? DBNull.Value });

        await conn.OpenAsync(ct);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<bool> IsServiceEnabledForServerAsync(string serverName, CancellationToken ct)
    {
        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT TOP (1) IsEnabled
FROM dbo.DbTaak_Scheduler_Task_services
WHERE UPPER(LTRIM(RTRIM(ServerName))) = UPPER(@ServerName)
ORDER BY TaskId DESC;";

        cmd.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.NVarChar, 200) { Value = serverName });

        await conn.OpenAsync(ct);
        var obj = await cmd.ExecuteScalarAsync(ct);

        if (obj == null || obj == DBNull.Value)
            return true;

        return Convert.ToInt32(obj) == 1;
    }

    public async Task<List<TaskRow>> FetchAndLockDueTasksAsync(AppConfig cfg, string instanceId, CancellationToken ct)
    {
        var list = new List<TaskRow>();

        using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
;WITH cte AS (
    SELECT TOP (@Top)
        TaskId, TaskName, TaskType, Payload,
        CronExpression, IntervalSeconds,
        TimeoutSeconds, MaxRetries, BackoffBaseSec, BackoffFactor,
        ServerName, Status, LockedBy, LockedAt
    FROM dbo.DbTaak_Scheduler_Task WITH (READPAST, UPDLOCK, ROWLOCK)
    WHERE IsEnabled = 1 
      AND NextRunAt <= SYSUTCDATETIME()
      AND (TaskType IS NULL OR UPPER(LTRIM(RTRIM(TaskType))) <> 'SYSTEM')
      AND NextRunAt <= SYSUTCDATETIME()
      AND (LockedAt IS NULL OR LockedAt < DATEADD(MINUTE, -30, SYSUTCDATETIME()))
      AND Status <> 'Running'
      AND (
            ServerName IS NULL OR LTRIM(RTRIM(ServerName)) = ''
            OR UPPER(LTRIM(RTRIM(ServerName))) = UPPER(@ThisServer)
          )
    ORDER BY NextRunAt ASC
)
UPDATE cte
SET Status   = 'Running',
    LockedBy = @LockedBy,
    LockedAt = SYSUTCDATETIME()
OUTPUT
    inserted.TaskId,
    inserted.TaskName,
    inserted.TaskType,
    inserted.Payload,
    inserted.CronExpression,
    inserted.IntervalSeconds,
    inserted.TimeoutSeconds,
    inserted.MaxRetries,
    inserted.BackoffBaseSec,
    inserted.BackoffFactor,
    inserted.ServerName;";

        cmd.Parameters.Add(new SqlParameter("@Top", SqlDbType.Int) { Value = cfg.MaxBatchPerPoll });
        cmd.Parameters.Add(new SqlParameter("@LockedBy", SqlDbType.NVarChar, 200) { Value = instanceId });
        cmd.Parameters.Add(new SqlParameter("@ThisServer", SqlDbType.NVarChar, 200) { Value = Environment.MachineName });

        using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var taskId = r.GetInt32(0);

            var taskName = r.IsDBNull(1) ? "" : r.GetString(1);
            var taskType = r.IsDBNull(2) ? "" : r.GetString(2);
            var payload = r.IsDBNull(3) ? "" : r.GetString(3);

            var cronExpr = r.IsDBNull(4) ? null : r.GetString(4);
            var interval = r.IsDBNull(5) ? (int?)null : r.GetInt32(5);

            var timeout = r.IsDBNull(6) ? (int?)null : r.GetInt32(6);

            var maxRetries = r.IsDBNull(7) ? 1 : r.GetInt32(7);
            var backoffBase = r.IsDBNull(8) ? 5 : r.GetInt32(8);
            var backoffFactor = r.IsDBNull(9) ? 2.0 : Convert.ToDouble(r.GetValue(9));

            var serverName = r.IsDBNull(10) ? null : r.GetString(10);

            list.Add(new TaskRow
            {
                TaskId = taskId,
                TaskName = taskName,
                TaskType = taskType,
                Payload = payload,

                CronExpression = cronExpr,
                IntervalSeconds = interval,

                TimeoutSeconds = timeout,
                MaxRetries = maxRetries,
                BackoffBaseSec = backoffBase,
                BackoffFactor = backoffFactor,

                ServerName = serverName
            });
        }

        return list;
    }

    public async Task<long> InsertRunAsync(int taskId, string serverName, CancellationToken ct)
    {
        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT dbo.DbTaak_Scheduler_TaskRun (TaskId, Result, ServerName)
OUTPUT inserted.RunId
VALUES (@TaskId, 'Running', @ServerName);";

        cmd.Parameters.Add(new SqlParameter("@TaskId", SqlDbType.Int) { Value = taskId });
        cmd.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.NVarChar, 200) { Value = serverName });

        await conn.OpenAsync(ct);
        var id = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(id);
    }

    public async Task SetRunPayloadProcessAsync(long runId, int pid, DateTime startedUtc, CancellationToken ct)
    {
        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.DbTaak_Scheduler_TaskRun
SET PayloadPid = @Pid,
    PayloadStartedAt = @StartedAt
WHERE RunId = @RunId;";
        cmd.Parameters.Add(new SqlParameter("@RunId", SqlDbType.BigInt) { Value = runId });
        cmd.Parameters.Add(new SqlParameter("@Pid", SqlDbType.Int) { Value = pid });
        cmd.Parameters.Add(new SqlParameter("@StartedAt", SqlDbType.DateTime2) { Value = startedUtc });

        await conn.OpenAsync(ct);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<List<(int TaskId, string TaskType, string TaskName, long RunId, int? Pid, DateTime LockedAtUtc)>>
        FetchStaleRunningWithPidAsync(int staleMinutes, string thisServer, CancellationToken ct)
    {
        var list = new List<(int, string, string, long, int?, DateTime)>();

        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT
    t.TaskId,
    t.TaskType,
    t.TaskName,
    r.RunId,
    r.PayloadPid,
    t.LockedAt
FROM dbo.DbTaak_Scheduler_Task t
JOIN dbo.DbTaak_Scheduler_TaskRun r ON r.TaskId = t.TaskId
WHERE t.Status = 'Running'
  AND t.LockedAt IS NOT NULL
  AND t.LockedAt < DATEADD(MINUTE, -@StaleMin, SYSUTCDATETIME())
  AND r.FinishedAt IS NULL
  AND r.ServerName = @Server
ORDER BY t.LockedAt ASC;";

        cmd.Parameters.Add(new SqlParameter("@StaleMin", SqlDbType.Int) { Value = staleMinutes });
        cmd.Parameters.Add(new SqlParameter("@Server", SqlDbType.NVarChar, 200) { Value = thisServer });

        await conn.OpenAsync(ct);
        using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var taskId = r.GetInt32(0);
            var taskType = r.IsDBNull(1) ? "" : r.GetString(1);
            var taskName = r.IsDBNull(2) ? "" : r.GetString(2);
            var runId = r.GetInt64(3);
            var pid = r.IsDBNull(4) ? (int?)null : r.GetInt32(4);
            var lockedAt = r.GetDateTime(5);

            list.Add((taskId, taskType, taskName, runId, pid, lockedAt));
        }

        return list;
    }

    public async Task<TaskRow> ReadTaskByIdAsync(int taskId, CancellationToken ct)
    {
        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT TaskId, TaskName, TaskType, Payload,
       CronExpression, IntervalSeconds,
       TimeoutSeconds, MaxRetries, BackoffBaseSec, BackoffFactor,
       ServerName
FROM dbo.DbTaak_Scheduler_Task
WHERE TaskId = @TaskId;";
        cmd.Parameters.Add(new SqlParameter("@TaskId", SqlDbType.Int) { Value = taskId });

        await conn.OpenAsync(ct);
        using var r = await cmd.ExecuteReaderAsync(ct);
        if (!await r.ReadAsync(ct))
            throw new Exception($"TaskId {taskId} bestaat niet.");

        return new TaskRow
        {
            TaskId = r.GetInt32(0),
            TaskName = r.IsDBNull(1) ? "" : r.GetString(1),
            TaskType = r.IsDBNull(2) ? "" : r.GetString(2),
            Payload = r.IsDBNull(3) ? "" : r.GetString(3),
            CronExpression = r.IsDBNull(4) ? null : r.GetString(4),
            IntervalSeconds = r.IsDBNull(5) ? (int?)null : r.GetInt32(5),
            TimeoutSeconds = r.IsDBNull(6) ? (int?)null : r.GetInt32(6),
            MaxRetries = r.IsDBNull(7) ? 1 : r.GetInt32(7),
            BackoffBaseSec = r.IsDBNull(8) ? 5 : r.GetInt32(8),
            BackoffFactor = r.IsDBNull(9) ? 2.0 : Convert.ToDouble(r.GetValue(9)),
            ServerName = r.IsDBNull(10) ? null : r.GetString(10),
        };
    }

    public async Task FinishRunAndUnlockAsync(TaskRow t, long runId, ExecResult r, string instanceId, DateTime nextRunUtc, string serverName, CancellationToken ct)
    {
        var resultText = r.TimedOut ? "Timeout" : r.Success ? "Success" : "Failed";

        using var conn = new SqlConnection(ConnectionString);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
UPDATE dbo.DbTaak_Scheduler_TaskRun
SET FinishedAt = SYSUTCDATETIME(),
    Result     = @Result,
    ExitCode   = @ExitCode,
    Output     = @Output,
    Error      = @Error,
    ServerName = @ServerName,
    PayloadPid = COALESCE(PayloadPid, @Pid)
WHERE RunId = @RunId;

UPDATE dbo.DbTaak_Scheduler_Task
SET Status      = CASE WHEN @Result='Success' THEN 'Idle' ELSE 'Error' END,
    LastRunAt    = SYSUTCDATETIME(),
    LastResult   = @Result,
    LastMessage  = LEFT(COALESCE(@Error, @Output, ''), 4000),
    NextRunAt    = @NextRunAt,
    LockedBy     = NULL,
    LockedAt     = NULL,
    UpdatedAt    = SYSUTCDATETIME()
WHERE TaskId = @TaskId
  AND LockedBy = @LockedBy;";

        cmd.Parameters.Add(new SqlParameter("@RunId", SqlDbType.BigInt) { Value = runId });
        cmd.Parameters.Add(new SqlParameter("@TaskId", SqlDbType.Int) { Value = t.TaskId });
        cmd.Parameters.Add(new SqlParameter("@LockedBy", SqlDbType.NVarChar, 200) { Value = instanceId });

        cmd.Parameters.Add(new SqlParameter("@Result", SqlDbType.NVarChar, 20) { Value = resultText });
        cmd.Parameters.Add(new SqlParameter("@ExitCode", SqlDbType.Int) { Value = (object?)r.ExitCode ?? DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@Output", SqlDbType.NVarChar, -1) { Value = (object?)r.Output ?? DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@Error", SqlDbType.NVarChar, -1) { Value = (object?)r.Error ?? DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@Pid", SqlDbType.Int) { Value = (object?)r.Pid ?? DBNull.Value });

        cmd.Parameters.Add(new SqlParameter("@NextRunAt", SqlDbType.DateTime2) { Value = nextRunUtc });
        cmd.Parameters.Add(new SqlParameter("@ServerName", SqlDbType.NVarChar, 200) { Value = serverName });

        await conn.OpenAsync(ct);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}


// ==============================
// Models / config
// ==============================

sealed class AppConfig
{
    public string SqlServer { get; set; } = "";
    public string Database { get; set; } = "";
    public bool UseIntegratedSecurity { get; set; } = true;
    public string? SqlUser { get; set; }
    public string? SqlPassword { get; set; }

    public int PollSeconds { get; set; } = 10;
    public int WorkerCount { get; set; } = 4;
    public int MaxBatchPerPoll { get; set; } = 20;
    public int DefaultTimeoutSeconds { get; set; } = 300;

    public List<string> AllowedRoots { get; set; } = new();

    public int WatchdogIntervalSeconds { get; set; } = 60; // hoe vaak checken
    public int WatchdogStaleMinutes { get; set; } = 30;    // Running + LockedAt ouder dan N => check PID
}

sealed class TaskRow
{
    public int TaskId { get; set; }
    public string TaskName { get; set; } = "";
    public string TaskType { get; set; } = "";
    public string Payload { get; set; } = "";

    public string? CronExpression { get; set; }
    public int? IntervalSeconds { get; set; }

    public int? TimeoutSeconds { get; set; }
    public int MaxRetries { get; set; } = 1;
    public int BackoffBaseSec { get; set; } = 5;
    public double BackoffFactor { get; set; } = 2.0;

    public string? ServerName { get; set; }
}

record ExecResult(bool Success, bool TimedOut, int? ExitCode, int? Pid, string Output, string Error);