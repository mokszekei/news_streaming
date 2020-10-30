import redis


class Redis():
    def __init__(self):
        self.con = redis.Redis(
            host='', 
            port=6379, 
            db=0,
            decode_responses=True #True: data type is str
        )
    def add(self,key,data):
        if self.con.setnx(key, data):
            print("Successfully add")
            return 1
        else:
            print("Data already exist")
            return 0