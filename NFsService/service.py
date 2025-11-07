#service.py

import win32serviceutil
import win32service
import win32event
import time
from fetch_nfse import fetch_nfse

class NFSeService(win32serviceutil.ServiceFramework):
    _svc_name_ = "NFSeService"
    _svc_display_name_ = "NFSe Fetch Service"
    _svc_description_ = "Serviço para buscar NFS-e de hora em hora."

    def __init__(self, args):
        super().__init__(args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.running = False 
        win32event.SetEvent(self.stop_event)  


    def SvcDoRun(self):
        while self.running:
            try:

                if win32event.WaitForSingleObject(self.stop_event, 1000) == win32event.WAIT_OBJECT_0:
                    break


                fetch_nfse()

            except Exception as e:
                with open("logs/service.log", "a") as log:
                    log.write(f"Erro no serviço: {e}\n")


            time.sleep(3600)  # 1 hora


if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(NFSeService)
