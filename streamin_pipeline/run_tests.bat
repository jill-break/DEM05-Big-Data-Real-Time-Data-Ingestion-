@echo off
REM Helper script to run tests
REM Assumes venv is in the parent directory or standard location

SET VENV_PATH=..\venv
IF EXIST ".\venv" SET VENV_PATH=.\venv

IF NOT EXIST "%VENV_PATH%\Scripts\activate.bat" (
    echo Virtual environment not found. Please create one.
    exit /b 1
)

call "%VENV_PATH%\Scripts\activate.bat"
echo Running Tests...
pip install -r requirements.txt
pytest tests/test_end_to_end.py
pause
