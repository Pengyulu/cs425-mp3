#!/usr/bin/python3
import socket
import sys
import threading
import time

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
        self.lock.acquire()
        if self.writelock:
            self.lock.release()
            return False
        elif self.readlock:
            self.reader.add(client)
            self.lock.release()
            return True
        else:
            self.readlock = True
            self.reader.add(client)
            self.lock.release()
            return True

    def r_release(self, client):
        self.lock.acquire()
        self.reader.discard(client)
        if len(self.reader) == 0:
            self.readlock = False
        self.lock.release()
        return True

    def w_acquire(self, client):
        self.lock.acquire()
        if self.writelock:
            self.lock.release()
            return False
            pass  # wait in the line or just return error msg
        elif self.readlock:
            if len(self.readlock) == 1 and client in self.reader:
                self.writelock = True
                self.writer = client
                self.reader.discard(client)
                self.readlock = False
                self.lock.release()
                return True
            else:
                # pass  # wait in the line or just return error msg
                self.lock.release()
                return False
        else:
            self.writelock = True
            self.writer = client
            self.lock.release()
            return True

    def w_release(self, client):
        self.lock.acquire()
        if self.writer == client:
            self.readlock = False
            self.writelock = False
            self.writer = None
            self.lock.release()
            return True
        else:
            self.lock.release()
            return False

    def release(self, client):
        self.r_release(client)
        self.w_release(client)
        return True

    def print_status(self):
        print("READLOCK",self.readlock,"WRITELOCK", self.writelock, "reader",self.reader, "writer",self.writer)


class Server:

    def __init__(self, id, IP, PORT):
        self.id = id
        self.pernament_acc = {}  # pernament bank branch account (committed)
        self.temp_acc = {}  # temporary bank account for each client
        # self.add_list = {}  # keep track of new account added by some client but not committed yet.
        # # ---- add list 是否可以直接用lock 替代? ----
        self.abort_bool = {}
        self.account_lock = {}  # account: Lock()
        self.al_lock = threading.Lock()
        self.client_lock_record = {}  # clientname: [Lock()]
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # socket_server.bind(('172.22.156.68', 1234))
        self.socket_server.bind((IP, PORT))
        self.socket_server.listen()
        self.thread_lock = threading.Lock()

    def handle_transaction(self, cnn, addr, msg_queue):
        # print("start handle trans")
        while (True):
            if len(msg_queue) != 0:
                # self.thread_lock.acquire()
                # print(msg_queue)
                msg = msg_queue.pop(0).strip().split()
                print("receive msg:", msg)
                # print(msg)
                client_name = msg[0]
                # temp_acc = self.temp_acc.get(client_name, None)
                # if temp_acc == None:
                #     temp_acc = {}
                #     self.temp_acc[client_name] = temp_acc
                self.temp_acc[client_name] = self.temp_acc.get(client_name, {})
                temp_acc = self.temp_acc[client_name]
                self.client_lock_record[client_name] = self.client_lock_record.get(client_name, set())
                my_lock_record = self.client_lock_record[client_name]
                if msg[1] == "BALANCE":
                    account = msg[2]
                    balance = temp_acc.get(account, None)
                    if balance:
                        send_msg = "BALANCE:" + account + " = " + str(balance)
                        print("SENDING:", send_msg)
                        send_msg = "{:<256}".format(send_msg)
                        cnn.send(send_msg.encode())
                    else:
                        per_balance = self.pernament_acc.get(account, None)
                        if per_balance:

                            # ---ask for read lock ---
                            self.al_lock.acquire()
                            self.account_lock[account] = self.account_lock.get(account, RWLock())
                            thislock = self.account_lock[account]
                            self.al_lock.release()
                            getlock = False
                            while not self.abort_bool.get(client_name, False):
                                time.sleep(0.01)
                                getlock = thislock.r_acquire(client_name)
                                if getlock:
                                    my_lock_record.add(thislock)
                                    break

                            if self.abort_bool.get(client_name, False):
                                self.abort_bool[client_name] = False
                                continue

                            send_msg = "BALANCE:" + account + " = " + str(per_balance)
                            print("SENDING:", send_msg)
                            send_msg = "{:<256}".format(send_msg)
                            cnn.send(send_msg.encode())
                        else:
                            # self.temp_acc[client_name] = {}
                            temp_acc.clear()
                            # to_delete = []
                            # for k, v in self.add_list.items():
                            #     if v == client_name:
                            #         to_delete.append(k)
                            # for item in to_delete:
                            #     self.add_list.pop(item, None)
                            send_msg = "NOT FOUND, ABORTED"
                            print("SENDING:", send_msg)
                            send_msg = "{:<256}".format(send_msg)
                            cnn.send(send_msg.encode())
                            # ---release all locks ---
                            for l in my_lock_record:
                                l.release(client_name)
                            my_lock_record.clear()

                elif msg[1] == "DEPOSIT":
                    account = msg[2]
                    amount = int(msg[3])
                    if account not in temp_acc:

                        # --- Acquire write lock here ---
                        self.al_lock.acquire()
                        self.account_lock[account] = self.account_lock.get(account, RWLock())
                        thislock = self.account_lock[account]
                        self.al_lock.release()
                        getlock = False
                        while not self.abort_bool.get(client_name, False):
                            time.sleep(0.01)
                            getlock = thislock.w_acquire(client_name)
                            #thislock.print_status()
                            if getlock:
                                my_lock_record.add(thislock)
                                break
                        if self.abort_bool.get(client_name, False):
                            self.abort_bool[client_name] = False
                            continue

                        if account not in self.pernament_acc:
                            # self.add_list[account] = client_name
                            temp_acc[account] = 0
                        else:
                            temp_acc[account] = self.pernament_acc[account]
                    temp_acc[account] += amount
                    send_msg = "OK"
                    print("SENDING:", send_msg)
                    send_msg = "{:<256}".format(send_msg)
                    # print("send_msg", send_msg)
                    cnn.send(send_msg.encode())

                elif msg[1] == "WITHDRAW":
                    account = msg[2]
                    amount = int(msg[3])
                    if account not in temp_acc:

                        # --- Acquire write lock here ---
                        self.al_lock.acquire()
                        self.account_lock[account] = self.account_lock.get(account, RWLock())
                        thislock = self.account_lock[account]
                        self.al_lock.release()
                        getlock = False
                        while not self.abort_bool.get(client_name, False) and not getlock:
                            time.sleep(0.01)
                            getlock = thislock.w_acquire(client_name)
                        if self.abort_bool.get(client_name, False):
                            self.abort_bool[client_name] = False
                            continue
                        else:
                            my_lock_record.add(thislock)

                        if account not in self.pernament_acc:
                            # self.add_list[account] = client_name
                            temp_acc[account] = 0
                        else:
                            temp_acc[account] = self.pernament_acc[account]
                    temp_acc[account] -= amount
                    send_msg = "OK"
                    print("SENDING:", send_msg)
                    send_msg = "{:<256}".format(send_msg)
                    cnn.send(send_msg.encode())
                elif msg[1] == "COMMIT":
                    abort = False
                    for acc, val in temp_acc.items():
                        if val < 0:
                            abort = True
                    if abort:
                        # self.temp_acc[client_name] = {}
                        temp_acc.clear()
                        # to_delete = []
                        # for k, v in self.add_list.items():
                        #     if v == client_name:
                        #         to_delete.append(k)
                        # for item in to_delete:
                        #     self.add_list.pop(item, None)
                        send_msg = "COMMIT ABORTED"
                        print("SENDING:", send_msg)
                        send_msg = "{:<256}".format(send_msg)
                        cnn.send(send_msg.encode())

                        # ---release all locks ---
                        for l in my_lock_record:
                            l.release(client_name)
                        my_lock_record.clear()

                    else:
                        send_msg = "COMMIT OK"
                        print("SENDING:", send_msg)
                        send_msg = "{:<256}".format(send_msg)
                        cnn.send(send_msg.encode())

                elif msg[1] == "COMMIT_CONFIRM":
                    for acc, val in temp_acc.items():
                        self.pernament_acc[acc] = val
                    temp_acc.clear()
                    # ---release all locks ---
                    print(my_lock_record)
                    for l in my_lock_record:
                        l.release(client_name)
                        #l.print_status()
                    my_lock_record.clear()

                # self.thread_lock.release()
                # if :
                #     self.temp_acc[client_name] = {}
                #     to_delete = []
                #     for k, v in self.add_list.items():
                #         if v == client_name:
                #             to_delete.append(k)
                #     for item in to_delete:
                #         self.add_list.pop(item, None)

    def receive_msg(self, cnn, addr):
        msg_queue = []
        threading.Thread(target=self.handle_transaction, args=(cnn, addr, msg_queue)).start()
        while True:
            received_msg = cnn.recv(256).decode()
            received_msg = received_msg.strip().split()
            # print(received_msg)
            if received_msg[1] == "ABORT":
                msg_queue.clear()
                # self.thread_lock.acquire()
                client_name = received_msg[0]
                self.abort_bool[received_msg[0]] = True
                time.sleep(0.5)
                # to_delete = []
                # for k, v in self.add_list.items():
                #     if v == client_name:
                #         to_delete.append(k)
                # for item in to_delete:
                #     self.add_list.pop(item, None)
                send_msg = "ABORTED"
                print("SENDING:", send_msg)
                send_msg = "{:<256}".format(send_msg)
                cnn.send(send_msg.encode())
                # ---release all locks including waiting ones---
                # self.thread_lock.release()
                self.temp_acc[client_name] = {}
                my_lock_record = self.client_lock_record[client_name]
                for l in my_lock_record:
                    l.release(client_name)
                my_lock_record.clear()
                self.abort_bool[received_msg[0]] = False
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
