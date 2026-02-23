cd D:\RGW\scheduler\RGW.Scheduler\RGW.Scheduler
"D:\Program Files\Microsoft Visual Studio\18\Community\dotnet\net8.0\runtime\dotnet.exe" --list-sdks
"D:\Program Files\Microsoft Visual Studio\18\Community\dotnet\net8.0\runtime\dotnet.exe" publish -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true /p:IncludeNativeLibrariesForSelfExtract=true
