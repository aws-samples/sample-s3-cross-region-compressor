@echo off
setlocal enabledelayedexpansion

:: Function to check if a command exists
call :check_command_exists uv
if !ERRORLEVEL! NEQ 0 (
    echo Warning: UV package manager not found
    echo UV is recommended for dependency management: https://github.com/astral-sh/uv
    echo Continuing without UV - will use existing requirements.txt files
    set USE_UV=false
) else (
    set USE_UV=true
)

:: Check for container engine (Finch or Docker)
set CONTAINER_ENGINE=

call :check_command_exists finch
if !ERRORLEVEL! EQU 0 (
    set CONTAINER_ENGINE=finch
    echo Using Finch as container engine
) else (
    call :check_command_exists docker
    if !ERRORLEVEL! EQU 0 (
        set CONTAINER_ENGINE=docker
        echo Using Docker as container engine
    ) else (
        echo Error: Neither Finch nor Docker found
        echo Please install either Finch (https://github.com/runfinch/finch) or Docker (https://www.docker.com/)
        exit /b 1
    )
)

:: Check if container engine is running and start if needed
if "%CONTAINER_ENGINE%"=="finch" (
    finch info > nul 2>&1
    if !ERRORLEVEL! NEQ 0 (
        echo Finch does not seem to be running, starting it now...
        finch vm start
        if !ERRORLEVEL! NEQ 0 (
            echo Failed to start Finch VM
            exit /b 1
        )
    )
) else if "%CONTAINER_ENGINE%"=="docker" (
    docker info > nul 2>&1
    if !ERRORLEVEL! NEQ 0 (
        echo Docker does not seem to be running, please start it manually
        exit /b 1
    )
)

:: Create dist directory if it doesn't exist
if not exist ".\bin\dist" mkdir ".\bin\dist"

:: Build the ARM containers
echo Building ARM64 containers...

:: Refresh dependencies
if "!USE_UV!"=="true" (
    echo Refreshing dependencies with UV...
    uv export --format requirements.txt --project ./bin/source_region -o ./bin/source_region/requirements.txt -q
    if !ERRORLEVEL! NEQ 0 (
        echo Failed to compile source_region dependencies
        exit /b 1
    )

    uv export --format requirements.txt --project ./bin/target_region -o ./bin/target_region/requirements.txt -q
    if !ERRORLEVEL! NEQ 0 (
        echo Failed to compile target_region dependencies
        exit /b 1
    )
) else (
    echo Using existing requirements.txt files...
)

:: Building containers with the appropriate engine
echo Building containers using %CONTAINER_ENGINE%...
%CONTAINER_ENGINE% build --platform=linux/arm64 -t source_region .\bin\source_region\
if !ERRORLEVEL! NEQ 0 (
    echo Failed to build source_region container
    exit /b 1
)

%CONTAINER_ENGINE% build --platform=linux/arm64 -t target_region .\bin\target_region\
if !ERRORLEVEL! NEQ 0 (
    echo Failed to build target_region container
    exit /b 1
)

:: Save the images to tar files using the appropriate engine
echo Saving container images to tar files...
if "%CONTAINER_ENGINE%"=="finch" (
    finch save source_region:latest > .\bin\dist\source_region.tar
    finch save target_region:latest > .\bin\dist\target_region.tar
) else if "%CONTAINER_ENGINE%"=="docker" (
    docker save source_region:latest -o .\bin\dist\source_region.tar
    docker save target_region:latest -o .\bin\dist\target_region.tar
)

echo Container build complete!
exit /b 0

:check_command_exists
where /q %1
exit /b !ERRORLEVEL!
