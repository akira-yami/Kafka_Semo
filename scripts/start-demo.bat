@echo off
setlocal
pushd "%~dp0.." || exit /b 1

docker compose up -d --wait --wait-timeout 90
if errorlevel 1 (
    popd
    exit /b 1
)

echo Kafka UI: http://localhost:8080
echo After Spring Boot starts, run the demo in another terminal.
call mvnw.cmd spring-boot:run
set "DEMO_EXIT=%ERRORLEVEL%"
popd
exit /b %DEMO_EXIT%
