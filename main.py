import socket
import sys
from mpi4py import MPI
import time as systime
import random
from enum import Enum

COLORS = [
    '\033[91m',
    '\033[92m',
    '\033[93m',
    '\033[94m',
    '\033[95m',
    '\033[96m',
    '\033[97m'  
]

RESET_COLOR = '\033[0m'

DEBUG_MODE = "-d" in sys.argv
HOSTNAME = socket.gethostname()

comm = MPI.COMM_WORLD
PID = comm.Get_rank()
NUM_PROCESSES = comm.Get_size()

global_time = 0
house_id = 0
house_queue = []
robber_queue = []
accepted = 0
waiting = 0

class Message:
    class Type(Enum):
        NEW = 0
        ADD_TO_ROBBER_QUEUE = 1
        SEND_HOUSES = 2
        OK_ROBBER_QUEUE = 3

    def __init__(self, msg_type=None, house_id=None, houses=None, time=None, pid=None):
        self.time = time if time is not None else self.increment_global_time()
        self.pid = pid if pid is not None else PID
        self.msg_type = msg_type
        self.house_id = house_id
        self.houses = houses

    def serialize(self):
        return {
            'time': self.time,
            'pid': self.pid,
            'type': self.msg_type.value,
            'house_id': self.house_id,
            'houses': self.houses
        }

    @staticmethod
    def deserialize(data):
        return Message(
            time=data.get('time', None) if 'time' in data else Message.increment_global_time(),
            pid=data.get('pid', None) if 'pid' in data else PID,
            msg_type=Message.Type(data['type']),
            house_id=data.get('house_id'),
            houses=data.get('houses')
        )
    
    @staticmethod
    def increment_global_time():
        global global_time
        if global_time is None:
            global_time = 0
        global_time += 1
        return global_time

def print_colored(message, force=False):
    if DEBUG_MODE or force:
        color = COLORS[PID % len(COLORS)]
        print(f"{color}[{PID}] {message}{RESET_COLOR}")

def NEW():
    global house_id
    message = Message(Message.Type.NEW, house_id=house_id)
    house_id += 1
    print_colored(f"Sending NEW message: {message.serialize()}")
    for i in range(NUM_PROCESSES):
        if i != PID:
            comm.send(message.serialize(), dest=i, tag=Message.Type.NEW.value)

def ADD_TO_ROBBER_QUEUE():
    message = Message(Message.Type.ADD_TO_ROBBER_QUEUE)
    print_colored(f"Sending ADD_TO_ROBBER_QUEUE message: {message.serialize()}")
    for i in range(NUM_PROCESSES):
        if i != PID and i != 0:
            comm.send(message.serialize(), dest=i, tag=Message.Type.ADD_TO_ROBBER_QUEUE.value)

def SEND_HOUSES():
    global house_queue, robber_queue
    houses = [(house_queue[i], robber_queue[i][1]) for i in range(min(len(house_queue), len(robber_queue)))]
    message = Message(Message.Type.SEND_HOUSES, houses=houses)
    print_colored(f"Sending SEND_HOUSES message: {message.serialize()}")
    for i in range(NUM_PROCESSES):
        if i != PID and i != 0:
            comm.send(message.serialize(), dest=i, tag=Message.Type.SEND_HOUSES.value)
    house_queue = house_queue[len(houses):]
    robber_queue = robber_queue[len(houses):]
    print_colored(f"Updated house_queue: {house_queue} and robber_queue {robber_queue}")

def OK_ROBBER_QUEUE(pid):
    message = Message(Message.Type.OK_ROBBER_QUEUE)
    print_colored(f"Sending OK_ROBBER_QUEUE to {pid} message: {message.serialize()}")
    comm.send(message.serialize(), dest=pid, tag=Message.Type.OK_ROBBER_QUEUE.value)

def RCV():
    global global_time, accepted, house_queue, robber_queue
    status = MPI.Status()
    while comm.Iprobe(source=MPI.ANY_SOURCE, status=status):
        data = comm.recv(source=status.Get_source(), tag=status.Get_tag())
        message = Message.deserialize(data)
        print_colored(f"Received message: {message.serialize()} with tag: {Message.Type(status.Get_tag()).name}")
        global_time = max(global_time, message.time)

        if status.Get_tag() == Message.Type.ADD_TO_ROBBER_QUEUE.value:
            robber_queue.append((message.time, message.pid))
            print_colored(f"Added to robber_queue: {robber_queue}")
            OK_ROBBER_QUEUE(message.pid)
        elif status.Get_tag() == Message.Type.OK_ROBBER_QUEUE.value:
            accepted += 1
            print_colored(f"Incremented accepted from {message.pid}: {accepted}")
        elif status.Get_tag() == Message.Type.SEND_HOUSES.value:
            for house in message.houses:
                if house[1] == PID:
                    accepted = 0
                    process_house(house[0])
            house_queue = house_queue[len(message.houses):]
            robber_queue = robber_queue[len(message.houses):]
            print_colored(f"Updated house_queue: {house_queue} and robber_queue {robber_queue}")
        elif status.Get_tag() == Message.Type.NEW.value and message.pid == 0:
            house_queue.append(message.house_id)
            print_colored(f"Added to house_queue: {house_queue}")

def process_house(house_id):
    print_colored(f"Process {PID} on {HOSTNAME} is processing house {house_id}", force=True)

def robber():
    global accepted
    while True:
        if PID not in [pid for _, pid in robber_queue]:
            ADD_TO_ROBBER_QUEUE()
            robber_queue.append((global_time, PID))
            print_colored(f"Added to robber_queue: {robber_queue}")
        RCV()
        if len(robber_queue) > 0 and len(house_queue) > 0 and accepted == NUM_PROCESSES - 2:
            robber_queue.sort()
            if robber_queue[0][1] == PID:
                accepted = 0
                to_process = house_queue[0]
                SEND_HOUSES()
                process_house(to_process)

def observer():
    while True:
        systime.sleep(random.uniform(0.1, 1.0))
        NEW()

if __name__ == "__main__":
    if PID == 0:
        observer()
    else:
        robber()
