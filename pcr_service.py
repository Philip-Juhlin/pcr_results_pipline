import os
import sys
import servicemanager
import win32event
import win32service
import win32serviceutil

class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PCRPipelineService"
    _svc_display_name_ = "PCR Export Processing Service"
    _svc_description_ = "Watches the raw PCR export folder and processes files automatically."
    _svc_type_ = win32service.SERVICE_AUTO_START

   
    _svc_account_ = "NT AUTHORITY\\NetworkService"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, servicemanager.PYS_SERVICE_STARTED, (self._svc_name_, ''))
        self.main()

    def main(self):
        # Add your Python script code here
        print('Starting Service...')
        import pipline 
        pipline.main()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyService)