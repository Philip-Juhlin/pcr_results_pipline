import win32serviceutil
import win32service
import win32event
import servicemanager
import time
from pathlib import Path
from pipline import watch_folder, POLL_INTERVAL 

class PCRPipelineService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PCRPipelineService"
    _svc_display_name_ = "PCR Export Processing Service"
    _svc_description_ = "Watches the raw PCR export folder and processes files automatically."

    def __init__(self, args):
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.stop_requested = False

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.stop_requested = True
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        self.main()

    def main(self):
        try:
            watch_folder(poll_interval=POLL_INTERVAL, stop_event=self.hWaitStop) 
        except Exception as e:
            servicemanager.LogErrorMsg(f"Pipeline error: {e}")

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(PCRPipelineService)
