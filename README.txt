RGW.Scheduler (self-contained exe)
=================================

Wat doet het?
- Pollt SQL Server voor "due" taken (NextRunAt <= now UTC)
- Lockt taken atomair (READPAST + UPDLOCK) zodat meerdere schedulers naast elkaar kunnen draaien
- Voert taken uit (CMD, POWERSHELL, SQL)
- Allowlist: CMD/POWERSHELL payload moet een absoluut pad zijn onder AllowedRoots
- Server targeting: RGW_Task.ServerName moet leeg/NULL zijn, of gelijk aan Environment.MachineName
- Quartz Cron: CronExpression (met secondenveld) wordt gebruikt om NextRunAt opnieuw te berekenen
- Retry: MaxRetries + exponential backoff (BackoffBaseSec, BackoffFactor)
- Service-mode: UseWindowsService()

Config
------
Plaats "rgwschedule.config" naast de exe (zelfde map). Voorbeeld staat in dit zipbestand.

Build/publish
-------------
1) dotnet restore
2) dotnet publish -c Release -r win-x64 /p:PublishSingleFile=true /p:SelfContained=true

Service installatie
-------------------
Zie scripts\install-service.cmd

SQL
---
Zie sql\schema.sql
