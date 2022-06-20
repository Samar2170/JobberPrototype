import socket
import pickle
import importlib
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
import sys 
sys.path.append(BASE_DIR)
class Worker:
    def __init__(self,task_dict) -> None:
        self.task_dict=task_dict
    
    def run(self):
        # mod_name = self.task_dict['location'].split('/')[-1]
        # mod_path = os.path.join(BASE_DIR,mod_name)
        mod_name = self.task_dict['location'].split('/')[-1]
        mod_name = mod_name.strip('.py')
        mod = importlib.import_module(mod_name)
        func = getattr(mod,self.task_dict['name'])
        print(func(*self.task_dict['args'],**self.task_dict['kwargs']))
    

class SuperVisor:
    def __init__(self) -> None:
        pass

    def start(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', 9999))
        s.listen(5)
        while True:
            conn, addr = s.accept()
            print('Connected by', addr)
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                # print(data)
                obj = pickle.loads(data)
                print(obj)
                worker = Worker(obj)
                worker.run()
                conn.send(data)
            conn.close()

if __name__ == '__main__':
    sup = SuperVisor()
    sup.start()