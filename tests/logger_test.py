from parallel_db.logger import get_logger, trace_call, filename
import unittest
from rich import progress

def simple_func():
    return 1

class TestLogger(unittest.TestCase):
    def test_init_handlers(self):
        logger = get_logger()
        print(logger.handlers)
        self.assertEqual(logger.hasHandlers(), True)
        self.assertIsInstance(logger.progress, progress.Progress) 
    
    # def test_init_no_handlers(self):
    #     logger = get_logger("test", log_consol=False, log_file=False, draw_progress=False)
    #     print(logger.handlers)
    #     self.assertEqual(logger.hasHandlers(), False)
    #     self.assertIsNone(logger.progress) 
        
    def test_trace_call(self):
        logger = get_logger(log_consol=False, log_file=False, draw_progress=False)
        res = trace_call(logger, simple_func)()
        self.assertEqual(res, 1)
        
    def test_dont_trace_call(self):
        logger = get_logger(log_consol=False, log_file=False, draw_progress=False)
        setattr(simple_func, 'custom_wrappers', ["trace_call"])
        res = trace_call(logger, simple_func)()
        self.assertEqual(res, 1)
        
    def test_log_text(self):
        logger = get_logger(log_consol=False, log_file=True, draw_progress=False)
        logger.info("info message")
        logger.error("error message")
        logger.warning("warning message")
        logger.critical("it's realy bad, sir")
        del logger
        with open(filename) as log_file:
            log_text = log_file.read()
        self.assertTrue(" - INFO]: info message" in log_text)
        self.assertTrue(" - ERROR]: error message" in log_text)
        self.assertTrue(" - WARNING]: warning message" in log_text)
        self.assertTrue(" - CRITICAL]: it's realy bad, sir" in log_text)
        
if __name__ == "__main__":
    unittest.main()