import importlib.util
if importlib.util.find_spec("vanna"):
    import vanna
    from vanna.remote import VannaDefault
    
class SingletonClass(object):
  def __new__(cls):
    if not hasattr(cls, 'instance'):
      cls.instance = super(SingletonClass, cls).__new__(cls)
    return cls.instance
    
class Helper(SingletonClass):
    def __init__(self):
        self.vanna = None
        if importlib.util.find_spec("vanna"):
            # self.vanna = VannaDefault(model="chinook")
            self.vanna = VannaDefault(model="chinook", api_key=vanna.get_api_key(""))
            
    def train_query(self, query: str):
        if self.vanna:
            self.vanna.train(query)
            return True
        return False
    
    def ask_vanna(self, question: str):
        if self.vanna:
            return self.vanna.ask(question, False)
        return None