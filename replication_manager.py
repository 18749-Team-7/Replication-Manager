import json
import random
import threading
import socket
import sys
import tkinter as tk

BLACK =     "\u001b[30m"
RED =       "\u001b[31m"
GREEN =     "\u001b[32m"
YELLOW =    "\u001b[33m"
BLUE =      "\u001b[34m"
MAGENTA =   "\u001b[35m"
CYAN =      "\u001b[36m"
WHITE =     "\u001b[37m"
RESET =     "\u001b[0m"
UP =        "\033[A"


class ReplicationManager:
    """
    Alien Tech.
    """

    def __init__(self, mode='active', rm_port=10001, gfd_port=10002):
        self.mode = mode
        self.membership = []
        self.primary = None
        self.rm_port = rm_port
        self.gfd_port = gfd_port
        self.gfd_isAlive = False

        self.replica_port = 15000

        self.RP_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        self.RP_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        self.host_ip = s.getsockname()[0]

        # Client parameters
        self.client_membership = {}
        self.client_port = 6666
        self.client_mem_mutex = threading.Lock() # Lock between add and remove client threads

        # GFD threads
        self.gfd_thread = threading.Thread(target=self.gfd_thread_func)
        self.gfd_heartbeat_thread = threading.Thread(target=self.gfd_heartbeat)

        # Client threads
        self.clients_thread = threading.Thread(target=self.add_clients)
        
        #self.gfd_thread.start()
        self.gfd_heartbeat_thread.start()
        self.clients_thread.start()
        print(RED + "GFD heartbeat thread started" + RESET)

        # start the chkpt window
        self.setup_chkpt_window()


    def modify_membership(self, gfd_info):
        """
        Function modifies the membership based on the updates from the GFD.
        param gfd_info: Dict() where keys-IPs of relica servers, values- updated status.
        # GFD only sends info regarding each replica at a given time.
        """
        member = gfd_info['server_ip']
        status = gfd_info['status']

        if status:
            if member not in self.membership:
                self.membership.append(member)

                # When there are no primaries
                if self.primary == None:
                    # Set a primary
                    self.primary = member
                
                # Message for alive replicas, informing about new replica
                msg = {}
                msg["type"] = "add_replicas"
                msg["ip_list"] = [member]
                msg["primary"] = self.primary
                self.send_replica_IPs(msg)

                # Message to new replica, informing about primary and all replicas
                msg_all = {}
                msg_all["type"] = "all_replicas"
                msg_all["ip_list"] = self.membership
                msg_all["primary"] = self.primary

                # sending updates to replicas
                self.send_replica_updates(msg, msg_all)

                print(GREEN + "The updated membership is: {}".format(self.membership))
        else:
            if member in self.membership:
                self.membership.remove(member)
                
                # If primary died, select a new primary
                if (member == self.primary):
                    if len(self.membership) == 0:
                        self.primary = None
                    else:
                        self.primary = self.pick_primary()

                msg = {}
                msg["type"] = "del_replicas"
                msg["ip_list"] = [member]
                msg["primary"] = self.primary
                
                self.send_replica_IPs(msg)

                # Inform other replicas about new primary
                msg_all = {}
                msg_all["type"] = "all_replicas"
                msg_all["ip_list"] = self.membership
                msg_all["primary"] = self.primary

                # sending updates to replicas
                self.send_replica_updates(msg, msg_all)
                
                print(GREEN + "The updated membership is: {}".format(self.membership))
        return 

    def msg_box_control(self, msg):
        if msg == "$client_members":
            print(MAGENTA + "RM -> Clients connected to RM: {}".format(self.client_membership.keys()) + RESET)
            return "break"

        if msg == "$help":
            print(MAGENTA + "RM -> Hi this is the Replication manager at {}".format(self.host_ip) + RESET)
            print(MAGENTA + "RM -> $client_members : Lists all the members that are connected" + RESET)
            print(MAGENTA + "RM -> $replica_members : Lists all the replica " + RESET)
            print(MAGENTA + "RM -> $primary : Prints the primary replica " + RESET)
            return "break"
        
        elif msg == "$replica_members":
            print(MAGENTA + "RM -> Replicas connected to RM: {}".format(self.membership) + RESET)

        elif msg == "$primary":
            print(MAGENTA + "RM -> Primary replica: {}".format(self.primary) + RESET)

        else:
            print(MAGENTA + "RM -> Error: Not an internal command")

    def change_chkpt_freq(self, event = None):
        time = self.input_field.get()
        self.input_user.set('')
        if time == "":
            return "break"
        if (time[0] == "$"):
            self.msg_box_control(time)
            return "break"

        try:
            time = float(time)
        except:
            print(RED + "Error: Time specified incorrectly" + RESET)
            return "break"


        # Create the message packet
        msg = {}
        msg["type"] = "chkpt_freq"
        msg["time"] = time

        data = json.dumps(msg)

        if (msg):
            print(UP) # Cover input() line with the chat line from the server.

            # Send message to primary replica with new chkpt freq
            # TODO: Mutex around this
            for replica_id in self.membership:
                self.RP_sock.sendto(data.encode("utf-8"), (replica_id, self.replica_port))

            print(MAGENTA + "Updated Checkpoint frequency to: {} s".format(time) + RESET)

        return "break"

    def setup_chkpt_window(self):
        # Create a window
        self.top = tk.Tk()
        self.top.title("Replication manager")

        # Create input text field
        self.input_user = tk.StringVar()
        self.input_field = tk.Entry(self.top, text=self.input_user)
        self.input_field.pack(side=tk.BOTTOM, fill=tk.X)

        # Create the frame for the text field
        self.input_field.bind("<Return>", self.change_chkpt_freq)

        self.top.bind("<Escape>", self.close_RM)

        # Start the chat window
        self.top.mainloop()

    def pick_primary(self):
        """
        Randomly pick a replica as primary from all members.
        """
        # Change this later!!!
        return random.choice(self.membership)

    
    def get_replica_ips(self):
        """
        Returns the IPs of all the current members.
        """
        return self.membership

    
    def gfd_heartbeat(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        gfd_count = 0

        server_address = (self.host_ip, self.gfd_port)
        print(RED + 'Starting listening for GFD Heartbeat at {}'.format(server_address) + RESET)
        try:
            sock.bind(server_address)
        
            # Listen for incoming connections
            sock.listen(1)
            connection, gfd_address = sock.accept()      
            print(RED + "GFD has connected from {}".format(gfd_address) + RESET)
            self.gfd_isAlive = True
            self.gfd_thread.start()
            # Waiting for gfd heart beat
            while True:
                try:
                    connection.settimeout(2)
                    _ = connection.recv(1024)
                    print(BLUE + "Received heartbeat from GFD at: {} | Heartbeat count: {}".format(gfd_address, gfd_count) + RESET)
                    gfd_count += 1
                    connection.settimeout(None)
                except socket.timeout:
                    print(RED + "Received timeout for GFD Heartbeat" + RESET)
                    self.gfd_isAlive = False
                    connection.close()
                    break
        except:
            print(RED + 'Heartbeat connection failed on replication manager {}'.format(server_address) + RESET)


    def gfd_thread_func(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Bind the socket to the replication port
        # host_name = socket.gethostname() 
        # host_ip = socket.gethostbyname(host_name) 

        server_address = (self.host_ip, self.rm_port)
        print(RED + 'Starting listening for GFD status at {}'.format(server_address) + RESET)
        sock.bind(server_address)
        
        # Listen for incoming connections
        sock.listen(1)

        connection, _ = sock.accept()
        connection.settimeout(None)

        try:
            # Waiting for gfd updates
            # If gfd is not alive, close the connection
            while self.gfd_isAlive:
                data = connection.recv(1024)         

                if data:
                    data2 = data
                    data2 = json.loads(data2.decode('utf-8'))
                    self.modify_membership(data2)
            connection.close()
                    
        except:
            # Anything fails, ie: replica server fails
            # print(e)
            # Clean up the connection
            connection.close()
    
    def add_clients(self):
        # Create a TCP/IP socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_address = (self.host_ip, self.client_port)
        print(RED + 'Starting listening for clients at {}'.format(server_address) + RESET)
        s.bind(server_address)
        
        # Listen for incoming connections
        s.listen(5)
        while True:
            # Accept a client connection
            conn, _ = s.accept()

            # Get the first packet which is add packet
            data = conn.recv(1024)
            msg = json.loads(data.decode("utf-8"))

            assert(msg["type"] == "add_client_rm")

            print(RED + "Client Connection from:", msg["client_id"] + RESET)

            threading.Thread(target=self.client_recv_thread, args=(conn, msg["client_id"])).start()

            self.client_mem_mutex.acquire()
            self.client_membership[msg["client_id"]] = conn
            self.client_mem_mutex.release()

            try:
                rm_msg = {}
                rm_msg["type"] = "add_replicas"
                rm_msg["ip_list"] = self.get_replica_ips()

                data = json.dumps(rm_msg)
                conn.send(data.encode("utf-8"))
            except:
                print(RED + "Connection with client {} closed unexpectedly".format(msg["client_id"]) + RESET)


            

    def send_replica_IPs(self, msg):
        # Send the message to all clients
        data = json.dumps(msg)
        self.client_mem_mutex.acquire()
        for client_id, s_client in self.client_membership.items():
            try: 
                s_client.send(data.encode("utf-8"))
            except:
                print(RED + "Connection with client {} closed unexpectedly".format(client_id) + RESET)
                del self.client_membership[client_id]
        self.client_mem_mutex.release()

        return

    # function to send replica list to replicas using UDP
    def send_replica_updates(self, msg, all_replicas):
        # Send the message to all replicas
        data_msg = json.dumps(msg)
        data_all_replicas = json.dumps(all_replicas)

        for replica_id in self.membership:
            # check which replica it is
            if (replica_id == msg["ip_list"][0]): # if it is new replica
                self.RP_sock.sendto(data_all_replicas.encode("utf-8"), (replica_id, self.replica_port))
            else:
                self.RP_sock.sendto(data_msg.encode("utf-8"), (replica_id, self.replica_port))


        return

    def client_recv_thread(self, conn, client_id):
        while True:
            data = conn.recv(1024)
            msg = json.loads(data.decode("utf-8"))

            assert(msg["type"] == "del_client_rm")

            print(RED + "Client Disconnecting:", msg["client_id"] + RESET)

            self.client_mem_mutex.acquire()
            del self.client_membership[msg["client_id"]]
            self.client_mem_mutex.release()

            conn.close()
            break

    def close_RM(self, event = None):
        self.top.destroy()
        sys.exit(1)

if __name__=="__main__":
    rm = ReplicationManager()