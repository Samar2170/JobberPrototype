import socket
import pickle
import importlib
import os
import importlib.util 
import sys
import redis

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
import sys 
sys.path.append(BASE_DIR)

import json
class Worker:
    def __init__(self,task_dict) -> None:
        self.task_dict=json.loads(task_dict)
    
    def run(self):
        
        mod_name = self.task_dict['location'].split('/')[-1]
        mod_name = mod_name.strip('.py')
        try:
            mod = importlib.import_module(mod_name)
            func = getattr(mod,self.task_dict['name'])

        except ModuleNotFoundError:
            try:
                loader = importlib.machinery.SourceFileLoader(mod_name,self.task_dict['location'])
                module = loader.load_module()
                func = getattr(module,self.task_dict['name'])
                args = self.task_dict['args']
                print(func(*args))
            except Exception as e:
                print(e)
                print('Module not found')
                return


        print(func(*self.task_dict['args'],**self.task_dict['kwargs']))
    

class SuperVisor:
    def __init__(self) -> None:
        pass

    # def start(self):
    #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     s.bind(('localhost', 9999))
    #     s.listen(5)
    #     while True:
    #         conn, addr = s.accept()
    #         print('Connected by', addr)
    #         while True:
    #             data = conn.recv(1024)
    #             if not data:
    #                 break
    #             # print(data)
    #             obj = pickle.loads(data)
    #             print(obj)
    #             worker = Worker(obj)
    #             worker.run()
    #             conn.send(data)
    #         conn.close()

    def start(self):
        self.redisInstance = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.sub = self.redisInstance.pubsub()
        while True:
            self.sub.subscribe('task')
            for message in self.sub.listen():
                if message['type'] == 'message':
                    obj = message['data']
                    print(obj)
                    worker = Worker(obj)
                    worker.run()

if __name__ == '__main__':
    sup = SuperVisor()
    sup.start()