import re
from schema import JobSchema
from broker import Broker
import pickle

def add(x,y):
    return x+y



class Job:
    broker = Broker()
    def __init__(self,func, *args,**kwargs) -> None:
        self.func = func 
        self.name = func.__name__
        self.args = args
        self.location = self.parse_location(str(func.__code__))
        self.task_dict = JobSchema(name=self.name,location=self.location,args=self.args, kwargs=kwargs)

    @classmethod
    def parse_location(cls,loc_str):


        pattern = r'file "(.*)", line (\d+)'
        match = re.search(pattern,loc_str)
        if match:
            file_name = match.group(1)
            line_no = match.group(2)
            return file_name
        else:
            return "NO_LOCATION","NO_LOCATION"

    def delay(self):
        obj = self.task_dict.dict()
        obj = pickle.dumps(obj)        
        self.broker.add_task(obj)
        return self.task_dict

if __name__=='__main__':
    job = Job(add,1,2)
    print(job.delay())
    job.broker.start()
