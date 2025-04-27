echo disable quick edit, but not working in Windows Terminal
reg add "HKCU\Console" /v QuickEdit /t REG_DWORD /d 0 /f
cd ..
uv run examples/subscribe_minute.py
pause