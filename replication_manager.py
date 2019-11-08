import json
import random
import threading


class ReplicationManager:
    """
    Alien Tech.
    """

    def __init__(self, mode='active'):
        self.mode = mode
        self.membership = []
        self.primary = None
        

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
        
        for member, status in gfd_info.item():
            if status:
                if member not in self.membership:
                    self.membership.append(member)
            else:
                self.membership.remove(member)

                # Elect a new primary if running on passive mode.
                if self.mode == 'passive':
                    if member == self.primary:
                        self.pick_primary()
                
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

    def tcp_server(self,port, logfile, verbose=False):
    try:
        
        host_ip = socket.gethostbyname(socket.gethostname())
        print(RED + "Starting chat server on " + str(host_ip) + ":" + str(port) + RESET)
        with open(logfile, 'a') as f:
            f.write("{}: Starting server\n".format(time.ctime()))

        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) # IPv4, TCPIP
        s.bind((host_ip, port))
        s.listen(5)

        while(True):
            # Accept a new connection
            conn, addr = s.accept()

            # Initiate a client listening thread
            threading.Thread(target=client_service_thread, args=(conn,addr, logfile, verbose)).start()

    except KeyboardInterrupt:
        # Closing the server
        s.close()
        print()
        print(RED + "Closing chat server on " + str(host_ip) + ":" + str(port) + RESET)
        with open(logfile, 'a') as f:
            f.write("{}: Closing server\n".format(time.ctime()))

if __name__ == '__main__':
    rep_mgr_obj = ReplicationManager()
    
