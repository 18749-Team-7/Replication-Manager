import threading 
import json
import argparse
import socket
import time
import replication_manager

def gfd_heartbeat(s):

    while(True):
        try:
            s.send("Heartbeat".encode('utf-8'))
            time.sleep(1)
        except:
            print("Error: Connection closed unexpectedly")
            s.close()

def gfd_membership(s):

    dicts = {}
    keys = range(4)
    for i in keys:
        dicts[i] = True
    while(True):
        try:
            x = json.dumps(dicts).encode('utf-8')
            s.send(x)
            time.sleep(10)
            
        except:
            print("Error: Connection closed unexpectedly gfd membership")
            
            s.close()
            break

def gfd_to_rm(ip,port1,port2):

    # Get a socket to connect to the server
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
    s.connect((ip, port2))
    threading.Thread(target=gfd_heartbeat,args=(s,)).start()
    # Get a socket to connect to the server
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
    s1.connect((ip, port1))
    threading.Thread(target=gfd_membership,args=(s1,)).start()


def get_args():
    parser = argparse.ArgumentParser()

    # IP, PORT, Username
    parser.add_argument('-ip', '--ip', help="RM IP Address", default='127.0.0.1')
    parser.add_argument('-p1', '--port1', help="RM port", type=int, default=10001)
    parser.add_argument('-p2', '--port2', help="Gfd heartbeat port",type=int, default=10002)
    
    # Parse the arguments
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    start_time = time.time()
    rep_mgr_obj = replication_manager.ReplicationManager()

    # Extract Arguments from the 
    args = get_args()

    # Start the Client
    gfd_to_rm(args.ip, args.port1,args.port2)
