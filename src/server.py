#!/usr/bin/python3
import socket
import sys
import threading

"""socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#socket_server.bind(('172.22.156.68', 1234))
socket_server.bind((ip, port))
socket_server.settimeout(100)
socket_server.listen(8)
sockets = []"""


class RWLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.readlock = False
        self.writelock = False
        self.reader = set()
        self.writer = None
        # self.readq= queue.Queue()
        # self.writeq = queue.Queue()

    def r_acquire(self, client):
        if self.writelock:
            pass  # wait in the line or just return error msg
        elif self.readlock:
            self.reader.add(client)
        else:
            self.readlock = True
            self.reader.add(client)

    def r_release(self, client):
        self.reader.discard(client)
        if len(self.reader) == 0:
            self.readlock = False

    def w_acquire(self, client):
        if self.writelock:
            pass  # wait in the line or just return error msg
        elif self.readlock:
            if len(self.readlock) == 1 and client in self.reader:
                self.writelock = True
                self.writer = client
                self.reader.discard(client)
                self.readlock = False
            else:
                pass  # wait in the line or just return error msg
        else:
            self.writelock = True
            self.writer = client

    def w_release(self):
        self.read = False
        self.write = False
        try:
            self.lock.release()
        except RuntimeError:
            return "The write lock is not locked."

    def release(self):
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
        self.thread_lock = threading.Lock()

    def handle_transaction(self, cnn, addr, msg_queue):
        # print("start handle trans")
        while (True):
            if len(msg_queue) != 0:
                self.thread_lock.acquire()
                # print(msg_queue)
                msg = msg_queue.pop(0).strip().split()
                # print(msg)
                client_name = msg[0]
                # temp_acc = self.temp_acc.get(client_name, None)
                # if temp_acc == None:
                #     temp_acc = {}
                #     self.temp_acc[client_name] = temp_acc
                self.temp_acc[client_name] = self.temp_acc.get(client_name, {})
                temp_acc = self.temp_acc.get(client_name, None)
                if msg[1] == "BALANCE":
                    account = msg[2]
                    balance = temp_acc.get(account, None)
                    if balance:
                        send_msg = "BALANCE:" + account + " = " + str(balance)
                        send_msg = "{:<256}".format(send_msg)
                        cnn.send(send_msg.encode())
                    else:
                        per_balance = self.pernament_acc.get(account, None)
                        if per_balance:
                            # ---ask for read lock ---
                            send_msg = "BALANCE:" + account + " = " + str(per_balance)
                            send_msg = "{:<256}".format(send_msg)
                            cnn.send(send_msg.encode())
                        else:
                            self.temp_acc[client_name] = {}
                            to_delete = []
                            for k, v in self.add_list.items():
                                if v == client_name:
                                    to_delete.append(k)
                            for item in to_delete:
                                self.add_list.pop(item, None)
                            send_msg = "NOT FOUND, ABORTED"
                            send_msg = "{:<256}".format(send_msg)
                            cnn.send(send_msg.encode())
                    # ---release all locks ---
                elif msg[1] == "DEPOSIT":
                    account = msg[2]
                    amount = int(msg[3])
                    if account not in temp_acc:
                        if account not in self.pernament_acc:
                            # ---- check add list and ask for lock here ----
                            self.add_list[account] = client_name
                            temp_acc[account] = 0
                        else:
                            # ---ask for write lock ---
                            temp_acc[account] = self.pernament_acc[account]
                    temp_acc[account] += amount
                    send_msg = "OK"
                    send_msg = "{:<256}".format(send_msg)
                    # print("send_msg", send_msg)
                    cnn.send(send_msg.encode())

                elif msg[1] == "WITHDRAW":
                    account = msg[2]
                    amount = int(msg[3])
                    if account not in temp_acc:
                        if account not in self.pernament_acc:
                            # ---- check add list and ask for lock here ----
                            self.add_list[account] = client_name
                            temp_acc[account] = 0
                        else:
                            # ---ask for write lock ---
                            temp_acc[account] = self.pernament_acc[account]
                    temp_acc[account] -= amount
                    send_msg = "OK"
                    send_msg = "{:<256}".format(send_msg)
                    cnn.send(send_msg.encode())
                elif msg[1] == "COMMIT":
                    abort = False
                    for acc, val in temp_acc.items():
                        if val < 0:
                            abort = True
                    if abort:
                        self.temp_acc[client_name] = {}
                        to_delete = []
                        for k, v in self.add_list.items():
                            if v == client_name:
                                to_delete.append(k)
                        for item in to_delete:
                            self.add_list.pop(item, None)
                        send_msg = "COMMIT ABORTED"
                        send_msg = "{:<256}".format(send_msg)
                        cnn.send(send_msg.encode())
                        # ---release all locks ---
                    else:
                        send_msg = "COMMIT OK"
                        send_msg = "{:<256}".format(send_msg)
                        cnn.send(send_msg.encode())

                elif msg[1] == "COMMIT_CONFIRM":
                    for acc, val in temp_acc.items():
                        self.pernament_acc[acc] = val
                    # --- release all locks ---
                self.thread_lock.release()

    def receive_msg(self, cnn, addr):
        msg_queue = []
        threading.Thread(target=self.handle_transaction, args=(cnn, addr, msg_queue)).start()
        while True:
            received_msg = cnn.recv(256).decode()
            received_msg = received_msg.strip().split()
            # print(received_msg)
            if received_msg[1] == "ABORT":
                msg_queue.clear()
                self.thread_lock.acquire()
                client_name = received_msg[0]  # ----CHANGE THIS----
                self.temp_acc[client_name] = {}
                to_delete = []
                for k, v in self.add_list.items():
                    if v == client_name:
                        to_delete.append(k)
                for item in to_delete:
                    self.add_list.pop(item, None)
                send_msg = "ABORTED"
                send_msg = "{:<256}".format(send_msg)
                cnn.send(send_msg.encode())
                # ---release all locks including waiting ones---
                self.thread_lock.release()
            else:
                msg_queue.append(" ".join(received_msg))

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
