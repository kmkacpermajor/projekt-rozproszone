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
house_id = 8
house_queue = [0,1,2,3,4,5,6,7]
robber_queue = []
accepted = []
waiting_ok = []
done = []

class Message:
    class Type(Enum):
        NEW = 0
        ADD_TO_ROBBER_QUEUE = 1
        SEND_HOUSES = 2
        OK_ROBBER_QUEUE = 3
        END = 4

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
        global_time += 1
        return global_time

def remove_houses(original_list, elements_tuples_to_remove):
    elements_to_remove = {element_tuple[0] for element_tuple in elements_tuples_to_remove}
    return [item for item in original_list if item not in elements_to_remove]

def remove_robbers(original_list, elements_tuples_to_remove):
    elements_to_remove = [element_tuple[1] for element_tuple in elements_tuples_to_remove]
    removal_count = {element: 0 for element in elements_to_remove}
    
    result = []
    for item in original_list:
        if item[1] in elements_to_remove:
            if removal_count[item[1]] == 0:
                removal_count[item[1]] += 1
            else:
                result.append(item)
        else:
            result.append(item)
    
    return result

def print_colored(message, force=False):
    if DEBUG_MODE or force:
        color = COLORS[PID % len(COLORS)]
        print(f"{color}[{global_time}] [{PID}] {message}{RESET_COLOR}")

def accept_to_robber_queue(message):
    global robber_queue
    robber_queue.append((message.time, message.pid))
    robber_queue.sort()
    print_colored(f"Added to robber_queue: {robber_queue}")
    OK_ROBBER_QUEUE(message.pid)

def END():
    message = Message(Message.Type.END)
    print_colored(f"Sending END message")
    for i in range(NUM_PROCESSES):
        if i != PID:
            comm.send(message.serialize(), dest=i, tag=Message.Type.END.value)
    MPI.Finalize()
    sys.exit(1)

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
    house_queue = remove_houses(house_queue, message.houses)
    robber_queue = remove_robbers(robber_queue, message.houses)
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
            accept_to_robber_queue(message)
        elif status.Get_tag() == Message.Type.OK_ROBBER_QUEUE.value:
            if message.pid not in accepted:
                accepted.append(message.pid)
                print_colored(f"Accepted from {message.pid}: {accepted}")
            else:
                waiting_ok.append(message)
        elif status.Get_tag() == Message.Type.SEND_HOUSES.value:
            done.extend([t[0] for t in message.houses])
            for house in message.houses:
                if house[1] == PID:
                    process_house(house[0])
            house_queue = remove_houses(house_queue, message.houses)
            robber_queue = remove_robbers(robber_queue, message.houses)
            print_colored(f"Updated house_queue: {house_queue} and robber_queue {robber_queue}")
        elif status.Get_tag() == Message.Type.NEW.value and message.pid == 0:
            if message.house_id not in done:
                house_queue.append(message.house_id)
                print_colored(f"Added to house_queue: {house_queue}")
        elif status.Get_tag() == Message.Type.END.value:
            MPI.Finalize()
            sys.exit(1)

def process_house(house_id):
    global accepted, global_time
    accepted = []
    seen_pids = []
    print_colored(f"Process {PID} on {HOSTNAME} is processing house {house_id}", force=True)
    for message in waiting_ok[:]:
        if message.pid not in seen_pids:
            seen_pids.append(message.pid)
            accepted.append(message.pid)
            waiting_ok.remove(message)
            print_colored(f"Accepted from {message.pid}: {accepted}")

def robber():
    global accepted
    while True:
        if PID not in [pid for _, pid in robber_queue]:
            ADD_TO_ROBBER_QUEUE()
            robber_queue.append((global_time, PID))
            robber_queue.sort()
            print_colored(f"Added to robber_queue: {robber_queue}")
        RCV()
        if len(set(accepted)) != len(accepted):
            print_colored(f"{len(robber_queue)}, {len(house_queue)}, {accepted}", force=True)
            END()
        if len(robber_queue) > 0 and len(house_queue) > 0 and len(accepted) == NUM_PROCESSES - 2:
            if robber_queue[0][1] == PID:
                print_colored("Im first in queue, sending houses", force=True)
                to_process = house_queue[0]
                SEND_HOUSES()
                process_house(to_process)

def observer():
    while True:
        systime.sleep(0.01)
        NEW()

if __name__ == "__main__":
    if PID == 0:
        observer()
    else:
        robber()
