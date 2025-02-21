echo disable quick edit, but not working in Windows Terminal
reg add "HKCU\Console" /v QuickEdit /t REG_DWORD /d 0 /f
CALL d:\Users\Kan\miniconda3\Scripts\activate.bat d:\Users\Kan\miniconda3\envs\py312
python subscribe_tick.py
pause