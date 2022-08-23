import os
from datetime import datetime

dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, '../../../log.txt')

def log_event(data: str):
    w = [f"{datetime.now()} : {data} \n"]
    print(w)
    f = open(filename, 'a')
    f.writelines('\n'.join(w))
    f.close() 