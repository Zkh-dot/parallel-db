import importlib.util
if importlib.util.find_spec("vanna"):
    import vanna
    from vanna.remote import VannaDefault

class Singleton (type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
    
class Helper(metaclass = Singleton):
    def __init__(self, email: str = None):
        self.vanna = None
        if importlib.util.find_spec("vanna") and email:
            # self.vanna = VannaDefault(model="chinook")
            self.vanna = VannaDefault(model="chinook", api_key=vanna.get_api_key(email=email))
            
    def train_on_query(self, query: str):
        if self.vanna:
            self.vanna.train(query)
            return True
        return False
    
    def ask_vanna(self, question: str):
        if self.vanna:
            return self.vanna.ask(question, False)
        return None