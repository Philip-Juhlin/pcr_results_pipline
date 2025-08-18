import win32serviceutil
import win32service
import win32event
import servicemanager
import time
import logging
from pathlib import Path
from pipline import watch_folder, POLL_INTERVAL

# Logging to a file for debugging
logging.basicConfig(
    filename=r"E:\pcr_results_pipline\service.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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
        logging.info("Service stop requested.")

    def SvcDoRun(self):
        # Report service is running immediately
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        logging.info("Service started.")
        self.main_loop()

    def main_loop(self):
        try:
            while not self.stop_requested:
                # Call your folder-watching function with a short interval
                watch_folder(poll_interval=POLL_INTERVAL, stop_event=self.hWaitStop)
                
                # Sleep a short while to avoid tight loop
                rc = win32event.WaitForSingleObject(self.hWaitStop, 1000)
                if rc == win32event.WAIT_OBJECT_0:
                    break
        except Exception as e:
            servicemanager.LogErrorMsg(f"Pipeline error: {e}")
            logging.exception("Pipeline error occurred.")

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(PCRPipelineService)
