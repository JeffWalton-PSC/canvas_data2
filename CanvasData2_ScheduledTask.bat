@echo on
echo "***** CanvasData2_ScheduledTask.bat batch file running" >> F:\Applications\Canvas\data2\logs\out.log
date /T >> F:\Applications\Canvas\data2\logs\out.log
time /T >> F:\Applications\Canvas\data2\logs\out.log

whoami 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1
echo "whoami" 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1

F:
echo "F:" 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1
cd F:\Applications\Canvas\data2
echo "cd" 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1

call C:\ProgramData\Anaconda3\condabin\activate.bat py312cd2 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1
echo "call activate" 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1
rem call conda list 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1

echo "python -m src " >> F:\Applications\Canvas\data2\logs\out.log
rem C:\ProgramData\Anaconda3\envs\py312nu\python.exe NewUserCreation_DepositedStudentsWithoutEmailAddress.py -v 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1
C:\ProgramData\Anaconda3\envs\py312nu\python.exe NewUserCreation_DepositedStudentsWithoutEmailAddress.py -v -w 1>> F:\Applications\Canvas\data2\logs\out.log 2>&1

rem pause
time /T >> F:\Applications\Canvas\data2\logs\out.log
echo "***** batch file exiting" >> F:\Applications\Canvas\data2\logs\out.log
echo "****************************************************************" >> F:\Applications\Canvas\data2\logs\out.log
