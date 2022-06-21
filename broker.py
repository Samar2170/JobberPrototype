from collections import deque
import socket
import time
# class Broker:
#     def __init__(self):
#         self.tasks = []
        
#     def add_task(self,task):
#         print(task)
#         self.tasks.append(task)
    
#     def start(self):
#         self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         self.s.connect(('localhost', 9999))

#         while True:
#             if len(self.tasks) ==0:
#                 time.sleep(1)
#                 continue
#             else:
#                 task = self.tasks.pop()
#                 print(task)
#                 self.s.send(task)
#                 # self.s.recv(1024)
#                 self.s.close()
#                 break

import redis
class NewBroker:
    def __init__(self):
        self.redisIns = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    def add_task(self,task_dict):
        self.redisIns.publish('task',task_dict)
        print(task_dict)
    
