' ʹ��WMI�������̣��Ա��ȡ��ʵ��PID
Set objWMIService = GetObject("winmgmts:\\.\root\cimv2")
Set objStartup = objWMIService.Get("Win32_ProcessStartup")
Set objConfig = objStartup.SpawnInstance_()
objConfig.ShowWindow = 0 ' ���ش�������

' �������̲���ȡPID
Dim intProcessID, objProcess
Set objProcess = objWMIService.Get("Win32_Process")
errReturn = objProcess.Create("java -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -jar MQTT_sub-1.0-SNAPSHOT.jar 8089", _
                             "D:\MQTT_Sub", _
                             objConfig, _
                             intProcessID)

' �������Ƿ�ɹ�����
If errReturn = 0 Then
    ' ����ʵ�Ľ���IDд���ļ�
    Set fso = CreateObject("Scripting.FileSystemObject")
    Set pidFile = fso.CreateTextFile("D:\MQTT_Sub\pid.txt", True)
    pidFile.Write intProcessID
    pidFile.Close
    Set fso = Nothing
Else
    WScript.Echo "��������ʧ�ܣ��������: " & errReturn
End If

Set objWMIService = Nothing
Set objStartup = Nothing
Set objConfig = Nothing
Set objProcess = Nothing
