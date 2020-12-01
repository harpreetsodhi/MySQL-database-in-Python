import logging
import os

class LoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, prefix):
        super(LoggerAdapter, self).__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        return '[%s] %s' % (self.prefix, msg), kwargs

class DataLogger:

    def __init__(self, user_name):
        self.user_name = user_name
        if not os.path.exists("Logs"):
            os.mkdir("Logs")
        logs_path = os.path.join(os.getcwd(), "Logs")
        self.GENERAL_LOGS = os.path.join(logs_path,"general.log")
        self.EVENT_LOGS = os.path.join(logs_path,'event.log')
        #Create two logger files    
        self.formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
   
    def event_log(self):
        # first file logger
        events_logger = logging.getLogger('Event_Log')
        handler1 = logging.FileHandler(self.EVENT_LOGS)
        handler1.setFormatter(self.formatter)
        events_logger.setLevel(logging.DEBUG)
        events_logger.addHandler(handler1)
        events_logger = LoggerAdapter(events_logger, self.user_name)

    def general_log(self):
        #second Logger
        general_logger = logging.getLogger("General_Log")
        handler2 = logging.FileHandler(self.GENERAL_LOGS)
        handler2.setFormatter(self.formatter)
        general_logger.setLevel(logging.DEBUG)
        general_logger.addHandler(handler2)
        general_logger = LoggerAdapter(general_logger, self.user_name)  

def main(name):
    a = DataLogger(name)
    a.event_log()
    a.general_log()

if __name__ == "__main__":
    main('jeyanth')
