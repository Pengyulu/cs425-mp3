#!/usr/local/bin/python3
import sys
import socket
import queue
import threading

class Client:
    def __init__(self, id, configFile):
        self.id = id
        self.config_file = configFile
        self.received_last = True
        self.begin_flag = False
        self.resend_abort = False
        self.message_to_user = None
        self.force_abort = False
        self.my_lock = threading.Lock()
        self.all_servers = {}
        self.all_sockets = {}
        self.server_involved = {}
        self.commit_ok = 0
        self.abort_ok = 0
        self.output_queue = queue.Queue()
        self.transactions_queue = queue.Queue()
        self.response_buffer = queue.Queue()
        self.current_trans_id = 0
        self.current_response_id = -1

    def read_config(self):
        file = open(self.config_file, 'r')
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            branch, ip, port = line.split()
            self.all_servers[branch] = [ip, port]

    def check_and_send(self):
        while True:
            if (not self.transactions_queue.empty()) and self.transactions_queue.queue[0] == "ABORT":
                self.my_lock.acquire()
                msg = str(self.id) + " " + self.transactions_queue.get()
                for server in self.all_servers:
                    msg = "{:<256}".format(msg)
                    res = self.all_sockets[server].send(msg.encode())
                self.received_last = False
                self.force_abort = False
                self.my_lock.release()
                continue
            self.my_lock.acquire()
            check_received = self.received_last
            self.my_lock.release()
            if (check_received):
                if not self.transactions_queue.empty() and self.transactions_queue.queue[0] == "BEGIN":
                    self.my_lock.acquire()
                    self.begin_flag = True
                    self.force_abort = False
                    self.transactions_queue.get()
                    self.my_lock.release()
                    print("OK")
                if not self.transactions_queue.empty():
                    trans = self.transactions_queue.get()
                    #print("trans: ", trans)
                    self.my_lock.acquire()
                    check_begin = self.begin_flag
                    self.my_lock.release()
                    #print("begin: ", self.begin_flag)
                    if check_begin:
                        trans_type, *element = trans.strip().split()
                        #print("trans_type: ", trans_type)
                        self.current_trans_id += 1
                        if trans_type == "DEPOSIT":
                            server_account, amount = element
                            server = server_account.split(".")[0]
                            account = server_account.split(".")[1]
                            msg = str(self.id) + " " + trans
                            msg = "{:<256}".format(msg)
                            #print("78: ", msg)
                            res = self.all_sockets[server].send(msg.encode())
                            self.my_lock.acquire()
                            self.received_last = False
                            self.my_lock.release()

                        elif trans_type == "WITHDRAW":
                            server_account, amount = element
                            server = server_account.split(".")[0]
                            account = server_account.split(".")[1]
                            msg = str(self.id) + " " + trans
                            msg = "{:<256}".format(msg)
                            #print("90: ", msg)
                            res = self.all_sockets[server].send(msg.encode())
                            self.my_lock.acquire()
                            self.received_last = False
                            self.my_lock.release()

                        elif trans_type == "BALANCE":
                            #print("element: ", element)
                            server = element[0].split(".")[0]
                            account = element[0].split(".")[1]
                            #print("server: ", server)
                            msg = str(self.id) + " " + trans
                            msg = "{:<256}".format(msg)
                            #print("103:", msg)
                            res = self.all_sockets[server].send(msg.encode())
                            self.my_lock.acquire()
                            self.received_last = False
                            self.my_lock.release()

                        elif trans_type == "COMMIT":
                            self.my_lock.acquire()
                            for server in self.all_servers:
                                msg = str(self.id) + " " + trans
                                msg = "{:<256}".format(msg)
                                #print("114: ", msg)
                                res = self.all_sockets[server].send(msg.encode())
                            self.received_last = False
                            self.my_lock.release()

                        elif trans_type == "ABORT":
                            self.my_lock.acquire()
                            for server in self.all_servers:
                                msg = str(self.id) + " " + trans
                                msg = "{:<256}".format(msg)
                                #print("124: ", msg)
                                res = self.all_sockets[server].send(msg.encode())
                            self.received_last = False
                            self.my_lock.release()


    def handle_user(self):
        for line in sys.stdin:
            if len(line.strip()) > 0:
                trans_type, *element = line.strip().split()
                self.transactions_queue.put(line.strip())

    def get_response(self):
        #print("start get response")
        while True:
            for server in self.all_sockets:
                my_socket = self.all_sockets[server]
                try:
                    response = my_socket.recv(256, socket.MSG_DONTWAIT).decode()
                    response = response.strip()
                except BlockingIOError as e:
                    response = None
                if response and len(response) != 0:
                    #print("receive response: ", response, ",", server)
                    msg_type = response.strip()
                    if msg_type == "NOT FOUND, ABORTED":
                        self.message_to_user = "NOT FOUND, ABORTED"
                        self.my_lock.acquire()
                        self.begin_flag = False
                        self.force_abort = True
                        to_send = "ABORT"
                        for server in self.all_servers:
                            msg = str(self.id) + " " + to_send
                            msg = "{:<256}".format(msg)
                            #print(msg)
                            res = self.all_sockets[server].send(msg.encode())
                        self.my_lock.release()

                    elif msg_type == "COMMIT ABORTED":
                        self.message_to_user = "COMMIT ABORTED"
                        self.my_lock.acquire()
                        self.begin_flag = False
                        self.force_abort = True
                        to_send = "ABORT"
                        for server in self.all_servers:
                            msg = str(self.id) + " " + to_send
                            msg = "{:<256}".format(msg)
                            #print(msg)
                            res = self.all_sockets[server].send(msg.encode())
                        self.my_lock.release()

                    if msg_type == "ABORTED":
                        self.my_lock.acquire()
                        self.begin_flag = False
                        self.abort_ok += 1
                        self.my_lock.release()
                        if not self.force_abort:
                            self.message_to_user = "ABORTED"

                    elif msg_type == "COMMIT OK":
                        self.my_lock.acquire()
                        self.begin_flag = False
                        self.commit_ok += 1
                        if self.commit_ok == len(self.all_servers):
                            for server in self.all_servers:
                                msg = str(self.id) + " " + "COMMIT_CONFIRM"
                                msg = "{:<256}".format(msg)
                                #print(msg)
                                res = self.all_sockets[server].send(msg.encode())
                            self.received_last = False
                            self.message_to_user = "COMMIT OK"
                        self.my_lock.release()


                    elif msg_type == "COMMITTED":
                        self.my_lock.acquire()
                        self.begin_flag = False
                        #del self.server_involved[server]
                        self.my_lock.release()

                    elif msg_type == "OK":
                        #print(msg_type)
                        self.my_lock.acquire()
                        self.received_last = True
                        self.message_to_user = "OK"
                        #print("line 201 FOR USER:", self.message_to_user)
                        print(self.message_to_user)
                        self.my_lock.release()

                    elif "BALANCE" in msg_type:
                        content = msg_type.split(":")[1]
                        #print(content)
                        self.my_lock.acquire()
                        self.message_to_user = content
                        self.received_last = True
                        #print("line 213 FOR USER:", self.message_to_user)
                        print(self.message_to_user)
                        self.my_lock.release()

                    if self.commit_ok == len(self.all_servers) or self.abort_ok == len(self.all_servers):
                        self.my_lock.acquire()
                        #print("line 217 FOR USER:", self.message_to_user)
                        print(self.message_to_user)
                        self.received_last = True
                        self.abort_ok = 0
                        self.commit_ok = 0
                        self.my_lock.release()

    def connect_to_all_servers(self):
        for branch in self.all_servers:
            host = self.all_servers[branch][0]
            port = int(self.all_servers[branch][1])
            #print(host, port)
            self.all_sockets[branch] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.all_sockets[branch].connect((host, port))

    def run(self):
        self.read_config()
        self.connect_to_all_servers()
        get_input = threading.Thread(target=self.handle_user)
        my_rcv = threading.Thread(target=self.get_response)
        my_send = threading.Thread(target=self.check_and_send)
        get_input.start()
        my_rcv.start()
        my_send.start()

def main():
    my_id = sys.argv[1]
    config_file = sys.argv[2]
    new_client = Client(my_id, config_file)
    new_client.run()

main()