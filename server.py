import socket
import sys
import threading

"""socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#socket_server.bind(('172.22.156.68', 1234))
socket_server.bind((ip, port))
socket_server.settimeout(100)
socket_server.listen(8)
sockets = []"""


class Lock:
    def __init__(self):
        pass


class Server:

    def __init__(self, id, IP, PORT):
        self.id = id
        self.pernament_acc = {}  # pernament bank branch account (committed)
        self.temp_acc = {}  # temporary bank account for each client (DO I NEED IT?
        self.add_list = {}  # keep track of new account added by some client but not committed yet.
        # ---- add list 是否可以直接用lock 替代? ----

        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # socket_server.bind(('172.22.156.68', 1234))
        self.socket_server.bind((IP, PORT))
        self.socket_server.listen()

    def handle_transaction(self, cnn, addr, msg_queue):
        print("start handle trans")
        while (True):
            if len(msg_queue) != 0:
                print(msg_queue)
                msg = msg_queue.pop().strip().split()
                print(msg)
                client_name = None  # ------ CHANGE THIS!!! -------
                temp_acc = self.temp_acc.get(client_name, None)
                if temp_acc == None:
                    temp_acc = {}
                    self.temp_acc[client_name] = temp_acc
                if msg[0] == "BALANCE":
                    account = msg[1]
                    balance = temp_acc.get(account, None)
                    if balance:
                        send_msg = account + " = " + str(balance)
                        cnn.send(send_msg.encode())
                    else:
                        per_balance = self.pernament_acc.get(account)
                        if per_balance:
                            # ---ask for read lock ---
                            send_msg = account + " = " + str(per_balance)
                            cnn.send(send_msg.encode())
                        else:
                            self.temp_acc[client_name] = {}
                            for k, v in self.add_list:
                                if v == client_name:
                                    self.add_list.pop(k)
                            send_msg = "NOT FOUND, ABORTED"
                            cnn.send(send_msg.encode())
                    # ---release all locks ---
                elif msg[0] == "DEPOSIT":
                    account = msg[1]
                    amount = int(msg[2])
                    if account not in temp_acc:
                        if account not in self.pernament_acc:
                            # ---- check add list and ask for lock here ----
                            self.add_list[account] = client_name
                            temp_acc[account] = 0
                        else:
                            # ---ask for write lock ---
                            temp_acc[account] = self.pernament_acc[account]
                    temp_acc[account] += amount

                elif msg[0] == "WITHDRAW":
                    account = msg[1]
                    amount = int(msg[2])
                    if account not in temp_acc:
                        if account not in self.pernament_acc:
                            # ---- check add list and ask for lock here ----
                            self.add_list[account] = client_name
                            temp_acc[account] = 0
                        else:
                            # ---ask for write lock ---
                            temp_acc[account] = self.pernament_acc[account]
                    temp_acc[account] -= amount
                elif msg[0] == "COMMIT":
                    abort = False
                    for acc, val in temp_acc:
                        if val < 0:
                            abort = True
                    if abort:
                        self.temp_acc[client_name] = {}
                        for k, v in self.add_list:
                            if v == client_name:
                                self.add_list.pop(k)
                        send_msg = "ABORTED"
                        cnn.send(send_msg.encode())
                        # ---release all locks ---
                    else:
                        send_msg = "COMMIT OK"
                        cnn.send(send_msg.encode())
                elif msg[0] == "COMMITREPLY":
                    for acc, val in temp_acc:
                        self.pernament_acc[acc] = val
                    # --- release all locks ---

    def receive_msg(self, cnn, addr):
        msg_queue = []
        threading.Thread(target=self.handle_transaction, args=(cnn, addr, msg_queue)).start()
        while True:
            received_msg = cnn.recv(24).decode()
            received_msg = received_msg.strip().split()
            print(received_msg)
            if received_msg[0] == "ABORT":
                client_name = None  # ----CHANGE THIS----
                self.temp_acc[client_name] = {}
                for k, v in self.add_list:
                    if v == client_name:
                        self.add_list.pop(k)
                send_msg = "ABORTED"
                cnn.send(send_msg.encode())
                # ---release all locks including waiting ones---
            else:
                msg_queue.append(" ".join(received_msg[1:]))

    def run(self):
        pass

    def listen(self):
        # self.socket_server.listen(10)
        while True:
            cnn, addr = self.socket_server.accept()
            threading.Thread(target=self.receive_msg, args=(cnn, addr)).start()


def main():
    id = sys.argv[1]
    config_file = sys.argv[2]
    f = open(config_file, 'r')
    for line in f:
        line = line.strip().split()
        if line[0] == id:
            ip = line[1]
            port = int(line[2])
            break
    server = Server(id, ip, port)
    server.listen()


main()