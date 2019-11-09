import json
import random
import threading
import socket


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
        print("GFD heartbeat thread started.")


    def establish_membership(self, gfd_init_info):
        """
        Function to establish initial membership of replicas based on GFD feedback.

        param gfd_init_info: Dict() where keys-IPs of relica servers, values- Init status.
        """

        for member, status in gfd_init_info.item():
            if status:
                self.membership.append(self.map(member))


    def modify_membership(self, gfd_info):
        """
        Function modifies the membership based on the updates from the GFD.

        param gfd_info: Dict() where keys-IPs of relica servers, values- updated status.
        """
        for member, status in gfd_info.items():
            if status:
                if member not in self.membership:
                    self.membership.append(member)
            else:
                self.membership.remove(member)

                # Send change_replica_ips request to the client 
                self.send_replica_IPs()

                # Elect a new primary if running on passive mode.
                if self.mode == 'passive':
                    if member == self.primary:
                        self.pick_primary()
        print("\n The current membership is :")
        print(self.membership)
                
        return 


    def pick_primary(self):
        """
        Randomly pick a replica as primary from all members.

        """
        # Change this later!!!
        self.primary = random.choice(self.membership)
        return

    
    def get_replica_ips(self):
        """
        Returns the IPs of all the current members.
        """
        return self.membership

    
    def gfd_heartbeat(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the replication port
        server_address = ('localhost', self.gfd_port)
        print('Starting listening on replication manager {} port {}'.format(*server_address))
        try:
            sock.bind(server_address)
        
            # Listen for incoming connections
            sock.listen(1)
            connection, gfd_address = sock.accept()      
            print('connection from', gfd_address)
            self.gfd_isAlive = True
            self.gfd_thread.start()
            # Waiting for gfd heart beat
            while True:
                try:
                    connection.settimeout(2)
                    data = connection.recv(1024)
                    print(data.decode('utf-8'))   
                    #print("Hearbeat received from GFD")
                    connection.settimeout(None)
                except socket.timeout:
                    print("Receive timeout")
                    self.gfd_isAlive = False
                    connection.close()
                    break
        except :
            print('Heartbeat connection failed on replication manager {} port {}'.format(*server_address))


    def gfd_thread_func(self):
        # Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the replication port
        server_address = ('localhost', self.rm_port)
        print('Starting listening on replication manager {} port {}'.format(*server_address))
        sock.bind(server_address)
        
        # Listen for incoming connections
        sock.listen(1)

        connection, gfd_address = sock.accept()
        connection.settimeout(None)

        try:
            print('connection for membership', gfd_address)

            # Waiting for gfd updates
            # If gfd is not alive, close the connection
            while self.gfd_isAlive:
                data = connection.recv(1024)
                # connection.settimeout(None)    
                print(data.decode('utf-8'))            
                #print('Updates received from GFD :{!r}'.format(data))

                if data:
                    print("Updates received from GFD : ")
                    print(data.decode('utf-8'))
                    data2 = data
                    data2 = json.loads(data2.decode('utf-8'))
                    self.modify_membership(data2)
            connection.close()
                    
        except Exception as e:
            # Anything fails, ie: replica server fails
            print(e)
            # Clean up the connection
            connection.close()
    
    def add_clients(self):
        # Create a TCP/IP socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # IPv4, TCPIP

        # Bind the socket to the replication port
        server_address = ('localhost', self.client_port)
        print('Starting listening for clients on replication manager {} port {}'.format(*server_address))
        s.bind(server_address)
        
        # Listen for incoming connections
        s.listen(5)
        while True:
            # Accept a client connection
            conn, client_addr = s.accept()

            # Get the first packet which is add packet
            data = conn.recv(1024)
            msg = json.loads(data.decode("utf-8"))

            assert(msg["type"] == "add_client_rm")

            print("Connection from:", msg["client_id"])

            self.client_mem_mutex.acquire()
            self.client_membership[msg["client_id"]] = conn
            self.client_mem_mutex.release()

            try:
                rm_msg = {}
                rm_msg["type"] = "replica_IPs"
                rm_msg["ip_list"] = self.get_replica_ips()

                data = json.dumps(rm_msg)
                conn.send(data.encode("utf-8"))
            except:
                print("Connection with client {} closed unexpectedly".format(msg["client_id"]))


            threading.Thread(target=self.client_recv_thread, args=(conn, msg["client_id"])).start()

    def send_replica_IPs(self):
        # Create the replica membership message
        msg = {}
        msg["type"] = "replica_IPs"
        msg["ip_list"] = self.get_replica_ips()

        data = json.dumps(msg)

        # Send the message to all clients
        self.client_mem_mutex.acquire()
        for client_id, s_client in self.client_membership.items():
            try: 
                s_client.send(data.encode("utf-8"))
            except:
                print("Connection with client {} closed unexpectedly".format(client_id))
        self.client_mem_mutex.release()

        return

    def client_recv_thread(self, conn, client_id):
        while True:
            data = conn.recv(1024)
            msg = json.loads(data.decode("utf-8"))

            assert(msg["type"] == "del_client_rm")

            print("Disconnecting:", msg["client_id"])

            self.client_mem_mutex.acquire()
            del self.client_membership[msg["client_id"]]
            self.client_mem_mutex.release()

            break



if __name__=="__main__":
    rm = ReplicationManager()

            

            

            