REM Kopieer publish output + rgwschedule.config naar bijvoorbeeld:
REM C:\Apps\RGW.Scheduler\

REM Install:
sc create RGW_Scheduler binPath= "D:\RGW\Scheduler\RGW_Scheduler.exe" start= auto
sc start RGW_Scheduler

REM Stop/verwijder:
REM sc stop RGW_Scheduler
REM sc delete RGW_Scheduler

REM Publish (voorbeeld):
REM dotnet publish -c Release -r win-x64 /p:PublishSingleFile=true /p:SelfContained=true
