@echo off
echo Restableciendo estado de teclas (sin PowerShell)...
REM --- crear script temporal en VBScript ---
set tmpvbs=%TEMP%\resetkeys.vbs
> "%tmpvbs%" echo Set sh = CreateObject("WScript.Shell")
>> "%tmpvbs%" echo sh.SendKeys "^{ESC}"
>> "%tmpvbs%" echo WScript.Sleep 150
>> "%tmpvbs%" echo sh.SendKeys "+^{ESC}"
>> "%tmpvbs%" echo WScript.Sleep 150
>> "%tmpvbs%" echo sh.SendKeys "{ESC}"

REM --- intentar ejecutar con cscript; si falla, probar wscript ---
"%SystemRoot%\System32\cscript.exe" //nologo "%tmpvbs%" 2>nul || "%SystemRoot%\System32\wscript.exe" "%tmpvbs%"

del "%tmpvbs%" >nul 2>&1
echo Listo. Comprueba si dejo de escribir solo.
pause
