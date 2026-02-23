REM Kopieer publish output + Db_TaakScheduler.config naar bijvoorbeeld:
REM C:\Apps\Db_TaakScheduler\

REM Install:
sc create Db_TaakScheduler_Scheduler binPath= "D:\Db_TaakScheduler\Db_TaakScheduler.exe" start= auto
sc start Db_TaakScheduler

REM Stop/verwijder:
REM sc stop Db_TaakScheduler
REM sc delete Db_TaakScheduler

REM Publish (voorbeeld):
REM dotnet publish -c Release -r win-x64 /p:PublishSingleFile=true /p:SelfContained=true
