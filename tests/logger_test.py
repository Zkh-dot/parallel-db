from parallel_db.logger import get_logger, trace_call
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
        
if __name__ == "__main__":
    unittest.main()