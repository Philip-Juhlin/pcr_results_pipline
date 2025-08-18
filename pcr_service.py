import win32serviceutil
import win32service
import win32event
import servicemanager
import time
import logging
import threading
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
        # Tell Windows the service is running
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        logging.info("Service started.")
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )

        # Start the folder watcher in a separate thread
        self.thread = threading.Thread(target=self.run_folder_watcher)
        self.thread.start()

        # Wait for stop signal
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        self.thread.join()
        logging.info("Service stopped.")

    def run_folder_watcher(self):
        try:
            watch_folder(poll_interval=POLL_INTERVAL, stop_event=self.hWaitStop)
        except Exception as e:
            logging.exception("Error in folder watcher")
            servicemanager.LogErrorMsg(f"Pipeline error: {e}")
