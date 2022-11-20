# class to implement a peer - can be a buyer or a seller
import Pyro5.server
import Pyro5.api
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock, BoundedSemaphore
import datetime
import time
import random
import re
import sys
import numpy as np
import json
import os.path
import atexit


# import threading as td

class Peer(Thread):
    """
    The Peer class represents a buyer or a seller within the P2P network.
    It has the following methods:
    1. get_random_neighbors - Create a neighbor list and assign neighbors to the peer
    2. connect_neighbors - Select at most max_neighbors random neighbors from the neighbor list and connect to them
    3. add_neighbor - Add a neighbor to the peer's neighbor list
    4. get_neighbor_len - Get the number of neighbors of the peer
    5. get_nameserver - Get the nameserver proxy
    6. run - Starts the market simulation
    7. lookup - Search for a product in the P2P network
    8. buy - Buy a product from a seller
    9. reply - Build the seller list for the buyer
    """

    def __init__(self, id, bully_id, role, product_count, products, hostname, max_neighbors, hopcount):
        """
        Construct a new 'Peer' object.

        :param id: The id of the peer
        :param role: The role of the peer
        :param product_count: The maximum number of products the peer can sell (if role is seller)
        :param products: The list of products that a peer can buy or sell
        :param hostname: The hostname of the peer
        :param max_neighbors: The maximum number of neighbors a peer can have
        :param hopcount: The maximum number of hops a peer can search for a product
        :return: returns nothing
        """

        Thread.__init__(self)
        self.id = id
        self.bully_id = bully_id
        self.hostname = hostname
        self.neighbors = {}
        self.trader = []
        self.role = role
        self.products = products
        self.product_name = self.products[random.randint(0, len(self.products)-1)]
        self.n = product_count
        self.product_count = product_count
        self.role = role
        self.ns = self.get_nameserver(hostname)
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.seller_list_lock = Lock()
        self.product_count_lock = Lock()
        self.seller_list = []
        self.max_neighbors = max_neighbors
        self.hopcount = hopcount
        # to store previous role when elected to trader
        self.prev_role = ""

        self.price = random.randint(0,10)
        # for trader
        self.seller_information = {}
        self.transaction_information = {}
        self.storage_semaphore = BoundedSemaphore(1)
        self.transaction_semaphore = BoundedSemaphore(1)

        # for seller
        self.seller_amount = 0
        
        # for failure condition on buyers
        self.buy_request_done = False
        self.reset_semaphore = BoundedSemaphore(1)
        self.buy_request_semaphore = BoundedSemaphore(1)
        self.fail_sem = BoundedSemaphore(1)
        self.sendWon = False
        self.recvWon = False
        self.recvOK = False
        self.won_sem = BoundedSemaphore(1)
        self.product_sem = BoundedSemaphore(1)

        self.clock_sem = BoundedSemaphore(1)
        self.clock = 0 + int(self.id[-1]) / 10.0
        self.buyer_list = [] # only for seller

    def get_random_neighbors(self):
        """
        Create a neighbor list and assign neighbors to the peer
        :return: returns nothing
        """
        neighbor_list = []
        ns_dict = self.ns.list()
        re_pattern = "seller[0-9]+|buyer[0-9]+"

        # Build a list of neighbors from the nameserver excluding the peer itself
        for id in ns_dict:
            if "NameServer" not in id and self.id != id and re.match(re_pattern, id):
                neighbor_list.append(id)
        self.connect_neighbors(neighbor_list)

        neighbor_list.clear()

    def connect_neighbors(self, neighbor_list):
        """
        Select at most max_neighbors random neighbors from the neighbor list and connect to them
        :param neighbor_list: The list of neighbors
        :return: nothing
        """
        if neighbor_list:

            for i in range(0, len(neighbor_list)):
                neighbor_id = neighbor_list[i]
                self.neighbors[neighbor_id] = self.ns.lookup(neighbor_id)

                with Pyro5.api.Proxy(self.neighbors[neighbor_id]) as neighbor:
                    try:
                        self.executor.submit(neighbor.add_neighbor, self.id)
                    except Exception as e:
                        print(datetime.datetime.now(), "Exception in connect_neighbors", e)

    @Pyro5.server.expose
    def add_neighbor(self, neighbor_id):
        """
        Add a neighbor to the peer's neighbor list
        :param neighbor_id: The id of the neighbor to add
        :return: nothing
        """
        # Complete bi-directional connections
        if neighbor_id not in self.neighbors.keys():
            self.neighbors[neighbor_id] = self.ns.lookup(neighbor_id)

    @Pyro5.server.expose
    def get_neighbor_len(self):
        """
        Get the number of neighbors of the peer
        :return: The number of neighbors
        """
        return len(self.neighbors.keys())

    def get_nameserver(self, ns_name):
        """
        Get the nameserver proxy
        :param ns_name: The hostname of the nameserver
        :return: The nameserver proxy
        """

        try:
            ns = Pyro5.core.locate_ns(host=ns_name)
            return ns
        except Exception as e:
            print(datetime.datetime.now(), "Exception in get_nameserver", e)

    def run(self):
        """
        Starts the market simulation
        :return: nothing
        """

        try:
            with Pyro5.server.Daemon(host=self.hostname) as daemon:
                uri = daemon.register(self)
                # Claim thread ownership of ns server proxy since each Pyro proxy is a thread
                self.ns._pyroClaimOwnership()
                self.ns.register(self.id, uri)

                if self.role == "buyer":
                    print(datetime.datetime.now(), self.id, "joins to buy ", self.product_name, " with bully id ", self.bully_id)
                else:
                    print(datetime.datetime.now(), self.id, "joins to sell ", self.product_name, " with bully id ", self.bully_id)

                # Peer starts listening for requests
                self.executor.submit(daemon.requestLoop)
                time.sleep(1)

                # Create a neighbor list and assign neighbors to the peer
                self.get_random_neighbors()

                print(self.id[-1])
                while self.id[-1] == '0':
                    self.startElection()
                
                while True:
                    time.sleep(1)

        except Exception as e:
            print("Exception in main", e.with_traceback())


    @Pyro5.server.expose
    def election_message(self,message,neighbor):
        print("Election message received from ",neighbor, " with message ",message)
        self.clock_sem.acquire()
        self.adjustClockValue(neighbor["clock"])
        self.clock_sem.release()
        if message == "Election":
            if self.recvOK or self.recvWon:
                with Pyro5.api.Proxy(self.neighbors[neighbor["id"]]) as neighbor_x:
                    neighbor_x.adjustClockValue(self.clock)
                    neighbor_x.election_message("OK",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
            else:
                with Pyro5.api.Proxy(self.neighbors[neighbor["id"]]) as neighbor_x:
                    neighbor_x.adjustClockValue(self.clock)
                    neighbor_x.election_message("OK",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
                # neighbor_ids = [int(x[-1]) for x in self.neighbors]
                # neighbor_ids = np.array(neighbor_ids)
                # greater_x = len(neighbor_ids[neighbor_ids > self.bully_id])
                neighbor_bully_ids = {}
                for neighbor_id in self.neighbors:
                    with Pyro5.api.Proxy(self.neighbors[neighbor_id]) as neighbor:
                        neighbor_bully_ids[neighbor_id] = neighbor.get_bully_id()
                        greater_bullies = [k for k, v in neighbor_bully_ids.items() if v > self.bully_id]
                if len(greater_bullies) > 0:
                    self.recvWon = False
                    self.recvOK = False
                    for ng in self.neighbors:
                        if int(ng[-1]) > self.bully_id:
                            if self.trader != [] and int(ng[-1]) == self.trader[0]["bully_id"]:
                                continue
                            else:
                                with Pyro5.api.Proxy(self.neighbors[ng]) as neighbor:
                                    print("election_message called at line 208")
                                    if self.recvOK == False or self.recvWon == False:
                                        self.clock_sem.acquire()
                                        self.forwardClockValue()
                                        self.clock_sem.release()
                                        neighbor.election_message("Election",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
                    time.sleep(2)
                    self.won_sem.acquire()
                    if not self.recvOK and not self.recvWon:
                        print("self.recvOK = ",self.recvOK)
                        print("self.recvWon = ",self.recvWon)
                        print("self.id = ",self.id)
                        self.sendWon = True
                        print("sendwonmessage called at line 221")
                        print("neigbor_bully_ids",neighbor_bully_ids)
                        print("self.bully_id",self.bully_id)
                        self.sendWonMessage()
                    else:
                        self.won_sem.release()

                else:
                    self.won_sem.acquire()
                    if self.sendWon == False:
                        self.sendWon = True
                        print("sendwonmessage called at line 229")
                        self.sendWonMessage()
                    else:
                        self.won_sem.release()
        elif message == "OK":
            self.recvOK = True
        elif message == "I Won":
            print(datetime.datetime.now(),"Peer ",self.id,": Election Won Message received")
            self.won_sem.acquire()
            self.recvWon = True
            # self.bully_id = random.randint(0, 200)
            self.won_sem.release()
            self.trader.clear()    
            self.trader.append(neighbor)
            # time.sleep(2)
            print(self.id," has self.recvWon = ",self.recvWon)
            print(self.id," has self.recvOK = ",self.recvOK)
            print(self.id, " ready to begin trading")
            # self.executor.submit(self.startTrading)
        # # TODO: add a fourth message saying "out of office" to restart the election process
        # elif message == "Out of Office":
        #     print(datetime.datetime.now(),"Peer ",self.id,": Out of Office message received")
        #     self.won_sem.acquire()
        #     self.recvWon = False
        #     self.recvOK = False
        #     self.won_sem.release()
        #     self.trader = []
        #     # time.sleep(2)
        #     if self.bully_id <= 2:
        #         print("Peer ",self.id,": starting the election")
        #         self.startElection(False)

    @Pyro5.server.expose
    def startElection(self):
        print(datetime.datetime.now(),self.id," starting election")
        time.sleep(2)
        
        for x in self.neighbors:
            with Pyro5.api.Proxy(self.neighbors[x]) as neighbor:
                neighbor.setDefaultFlags()
                print(neighbor.tradingMessage())

        self.setDefaultFlags()
       
        neighbor_bully_ids = {}
        for neighbor_id in self.neighbors:
            with Pyro5.api.Proxy(self.neighbors[neighbor_id]) as neighbor:
                neighbor_bully_ids[neighbor_id] = neighbor.get_bully_id()
                greater_bullies = [k for k, v in neighbor_bully_ids.items() if v > self.bully_id]
        if len(greater_bullies) > 0:
            self.recvWon = False
            self.recvOK = False
            for ng in greater_bullies:
                # if int(ng[-1]) > self.bully_id:
                #     if self.trader != [] and int(ng[-1]) == self.trader[0]["bully_id"]:
                #         continue
                #     else:
                with Pyro5.api.Proxy(self.neighbors[ng]) as neighbor:
                    print("election_message called at line 300")
                    if self.recvOK == False and self.recvWon == False:
                        print("self.recvOK = ",self.recvOK)
                        print("self.recvWon = ",self.recvWon)
                        print("self.id = ",self.id)
                        self.clock_sem.acquire()
                        self.forwardClockValue()
                        self.clock_sem.release()
                        neighbor.election_message("Election",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
            time.sleep(2)
            # if peer doesn't receive any OK or Won message, it is the coordinator
            self.won_sem.acquire()
            if self.recvOK == False and self.recvWon == False:
                self.sendWon = True
                # semaphore is released in the sendWonMessage method
                print("sendwonmessage called at line 288")
                self.sendWonMessage()
            else:
                self.won_sem.release()
        # if no greater bully id is found, it is the coordinator
        else:
            self.won_sem.acquire()
            self.sendWon = True
            # semaphore is released in the sendWonMessage method
            print("sendwonmessage called at line 297")
            self.sendWonMessage()

    @Pyro5.server.expose
    def setDefaultFlags(self):
        self.recvOK = False
        self.recvWon = False
        self.sendWon = False
        self.bully_id = random.randint(0,200)

    @Pyro5.server.expose
    def startTrading(self):
        self.reset_semaphore.acquire()
        # self.recvOK = False
        # self.recvWon = False
        self.reset_semaphore.release()
        print(datetime.datetime.now(),"trading entered by ",self.id)
        time.sleep(2)
        if self.role == "seller":
            # with self.trader[0]['id']
            print(datetime.datetime.now(),self.id," is registering its market for ",self.product_name)
            with Pyro5.api.Proxy(self.neighbors[self.trader[0]['id']]) as neighbor:
                neighbor.register_products({"seller":{"bully_id":self.bully_id,"id":self.id},"product_name": self.product_name,"product_count":self.product_count,"product_price":self.price,"seller_amount":self.seller_amount})
        
        # elif self.role == "buyer":
        #     #TODO: remove the hardcoded 6 value
        #     # time.sleep(8.5+self.bully_id/6.0)
        #     while True:
        #         self.buy_request_done = False
        #         ## TODO
        #         ## TODO: add item count
        #         print(datetime.datetime.now(),self.id," is looking to buy ",self.product_name)
        #         with Pyro5.api.Proxy(self.neighbors[self.trader[0]['id']]) as neighbor:
        #             self.executor.submit(neighbor.trading_lookup, {"bully_id":self.bully_id,"id":self.id}, self.product_name)
        #             # neighbor.trading_lookup({"bully_id":self.bully_id,"id":self.id},desired_item)
        #         # time.sleep(16)
        #         self.buy_request_semaphore.acquire()
        #         print(datetime.datetime.now(),"Peer ",self.id," ",self.buy_request_done)
        #         if not self.buy_request_done:
        #             self.startElection()
        #         self.buy_request_semaphore.release()
        elif self.role == "trader":
            pass

    @Pyro5.server.expose
    def sendWonMessage(self):
        """
        Send a won message to all neighbors
        :return: nothing
        """
        print(datetime.datetime.now(),"Dear buyers and sellers, my bully id is ",self.bully_id," (i.e ",self.id,")and I am the new coordinator")
        self.recvWon = True
        self.trader.append({"bully_id":self.bully_id,"id":self.id})
        self.prev_role = self.role
        self.role = "trader"
        self.load_state()
        self.won_sem.release()
        for neighbor_name in self.neighbors:
                print("sending to my neighbor: ",neighbor_name)
                with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                    self.clock_sem.acquire()
                    self.forwardClockValue()
                    self.clock_sem.release()
                    neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
                # with Pyro5.client.Proxy(self.neighbors[neighbor_name]) as neighbor:
                #     # Claim ownership of the seller proxy since each Pyro proxy is a thread
                #     neighbor._pyroClaimOwnership()
                #     # seller.buy(self.id)
                #     neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id})

        # retire_chance = np.random.choice([i for i in range(1,21)],1)[0]
        # if retire_chance <= 7:
        #     self.won_sem.acquire()
        #     self.sendOutMessage()
        # else:
        print("coordinator notified all neighbors.")

        # if os.path.exists("transactions.json"):
        #     print("completing previous transactions")
        #     with open("transactions.json","r") as transact:
        #         data = json.load(transact)
        #         for k in data.keys():
        #             with Pyro5.api.Proxy(self.neighbors[k]) as neighbor:
        #                 self.executor.submit(self.trading_lookup, neighbor.tradingMessage(), neighbor.productName())

        print("sellers register products with trader")
        for neighbor_name in self.neighbors:
            with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                if "seller" in neighbor_name:
                    neighbor.startTrading()

        print("buyers start trading on threads")
        try:
            while self.role == "trader":
                for neighbor_name in self.neighbors:
                    with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                        if "buyer" in neighbor_name:
                            print('within if')
                            buyer_product_count = random.randint(1,4)
                            self.executor.submit(self.trading_lookup, neighbor.tradingMessage(), neighbor.productName(),buyer_product_count)
                    if self.role != "trader":
                        print("trader role changed")
                        break
                print(self.id)
                print(self.role)
                print("starting next cycle")
        except Exception as e:
            print("Exception in starting trading lookup", e)

    @Pyro5.server.expose
    def tradingMessage(self):
        return {"bully_id":self.bully_id,"id":self.id, "clock":self.clock}

    @Pyro5.server.expose
    def get_bully_id(self):
        return self.bully_id

    @Pyro5.server.expose
    def productName(self):
        return self.product_name

    @Pyro5.server.expose
    def adjustClockValue(self, other):
        self.clock = max(int(self.clock), int(other)) + 1 + int(self.id[-1]) / 10.0

    @Pyro5.server.expose
    def forwardClockValue(self):
        self.clock += 1

    @Pyro5.server.expose
    def getClock(self):
        return self.clock

    @Pyro5.server.expose
    def trading_lookup(self,buyer_info,item,item_count):
        ##TODO
        print("in the trading lookup")
        self.fail_sem.acquire()
        print("within trading lookup", self.role)

        if self.role == "trader":
            print(datetime.datetime.now(),"trader ",self.id," received request from buyer ",buyer_info["id"], "for product ",item,"(",item_count,")")
            sellers = []
            transactions_file = "transactions.json"
            tlog = {"buyer":buyer_info["id"],"seller":"_","product":item,"completed":False}
            ## TODO: define put log
            self.put_log(tlog,transactions_file,False,True)
            retire_chance = np.random.choice([i for i in range(1,21)],1)[0]
            if retire_chance <= 5:
                self.won_sem.acquire()
                print(datetime.datetime.now(),"[DEBUG] trader ",self.id, " is vacating the coordinator position silently")
                self.recvWon = False
                self.recvOK = False
                self.sendWon = False
                self.trader= []
                self.role = self.prev_role
                self.prev_role = ""
                self.trader.clear()
                print("[DEBUG] trader assumed new role: ", self.role)
                self.won_sem.release()
                self.fail_sem.release()
                return
            for peer_id in self.seller_information.keys():
                if self.seller_information[peer_id]["product_name"] == item and self.id != peer_id:
                    sellers.append(self.seller_information[peer_id])

            if len(sellers)>0:
                # select the first available
                # seller = sellers[0]
                found = False
                f_sell = sellers[0]
                for sl in sellers:
                    sl_id = sl["seller"]["id"]
                    if self.seller_information[sl_id]["product_count"] < item_count:
                        continue
                    else:
                        found = True
                        f_sell = sl
                        break
                if not found:
                    with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                        neighbor.transaction(item,buyer_info["id"],"",self.id,False,True,0,item_count)
                        self.put_log(tlog,transactions_file,True,False)
                        self.fail_sem.release()
                        return
                else:
                    seller = f_sell
                    print("[DEBUG] seller: ", seller, " chosen for transaction")
                    seller_peer_id = seller["seller"]["id"]
                    print("[DEBUG] seller peer id: ", seller_peer_id)
                    # TODO: change product count according to buyer needs
                    try:
                        print(self.seller_information)
                        # if self.seller_information[seller_peer_id]["product_count"] < item_count:
                        #     with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                        #         neighbor.transaction(item,buyer_info["id"],"",self.id,False,True,0)
                        #         self.put_log(tlog,transactions_file,True,False)
                        #         self.fail_sem.release()
                        #         return
                        # else:
                        self.seller_information[seller_peer_id]["product_count"] -= item_count
                        self.seller_information[seller_peer_id]["seller_amount"] += item_count*seller['product_price']
                        self.seller_information[seller_peer_id]["buyer_list"].append(buyer_info["id"])
                        with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as seller_add:
                            seller_add.addBuyer(buyer_info["id"])
                        with open("seller_information.json","w") as sell:
                            json.dump(self.seller_information,sell)
                        tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"completed":False}
                        self.put_log(tlog,transactions_file,False,True)
                    except Exception as e:
                        print("[DEBUG] error in updating transaction information: ", e)
                
                # seller's information should update
                with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as neighbor:
                        print("updating seller information")
                        neighbor.transaction(item,buyer_info, seller_peer_id,self.id,False,False,0,item_count)

                tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"completed":True}
                self.put_log(tlog,transactions_file,True,True)
                # buyer should let know the success
                with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                        print("Start of transaction of buyer")
                        print(seller)
                        neighbor.transaction(item,buyer_info, seller_peer_id,self.id,True,False,seller['product_price'],item_count)
            else:
                with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                        neighbor.transaction(item,buyer_info["id"],"",self.id,False,False,0,item_count)
                        self.put_log(tlog,transactions_file,True,False)
        self.fail_sem.release()

    @Pyro5.server.expose
    def addBuyer(self, buyer_id):
        self.buyer_list.append(buyer_id)

    @Pyro5.server.expose
    def transaction(self,product_name,buyer_info,seller_id,trader_id,buyer_success,insufficient,sell_success,item_cnt):
        if self.role == "seller" and self.product_name == product_name:
            print(datetime.datetime.now(),self.id," received request from trader ",trader_id," for item ",product_name,"(",item_cnt,")")
            buyer_clocks = {}

            print("buyer_list: ", self.buyer_list)
            for buyer in self.buyer_list:
                with Pyro5.api.Proxy(self.neighbors[buyer]) as neighbor:
                    buyer_clocks[buyer] = neighbor.getClock()
            print("[DEBUG] buyer clocks: ", buyer_clocks)
            max_key = max(buyer_clocks, key=buyer_clocks.get)
            print("[DEBUG] buyer with max clock: ", buyer_clocks[max_key])
            if max_key == buyer_info["id"]:
                print("within max key condition")
                self.product_count -= item_cnt
            self.seller_amount += item_cnt*self.price
            if self.product_count == 0:
                self.product_name = self.products[random.randint(0, len(self.products)-1)]
                self.product_count = 10
                with Pyro5.api.Proxy(self.neighbors[self.trader[0]['id']]) as neighbor:
                    neighbor.register_products({"seller":{"bully_id":self.bully_id,"id":self.id},"product_name": self.product_name,"product_count":self.product_count,"product_price":self.price,"seller_amount":self.seller_amount})
                print("end of max key")
        elif self.role == "buyer":
            print("inside buyer")
            if buyer_success:
                print(datetime.datetime.now(),self.id," has got the ",item_cnt," items of ",product_name," for price ",sell_success)
                self.buy_request_semaphore.acquire()
                self.buy_request_done = True
                self.buy_request_semaphore.release()
            else:
                if insufficient:
                    print(datetime.datetime.now(),self.id," has extra demand than the current supply of ",product_name)
                else:
                    print(datetime.datetime.now(),self.id," could not find any seller for the item ",product_name)
                    self.buy_request_semaphore.acquire()
                    self.buy_request_done = True
                    self.buy_request_semaphore.release()

            self.product_sem.acquire()
            self.product_name = self.products[random.randint(0, len(self.products)-1)]
            print(datetime.datetime.now(), self.id, " now buying ", self.product_name)
            self.product_sem.release()

    @Pyro5.server.expose
    def register_products(self,seller_info):
        self.storage_semaphore.acquire()
        peer_id = seller_info["seller"]["id"]
        seller_info["buyer_list"] = []
        self.seller_information[peer_id] = seller_info

        with open("seller_information.json","w") as sell:
            json.dump(self.seller_information,sell)
        self.storage_semaphore.release()
    
    @Pyro5.server.expose
    def load_state(self):
        if os.path.exists("seller_information.json"):
            self.storage_semaphore.acquire()
            with open("seller_information.json") as sell:
                self.seller_information = json.load(sell)
            # if self.id in self.seller_information.keys():
            self.storage_semaphore.release()
        if os.path.exists("transactions.json"):
            self.transaction_semaphore.acquire()
            with open("transactions.json") as transact:
                self.transaction_information = json.load(transact)
            self.transaction_semaphore.release()
    @Pyro5.server.expose
    def put_log(self,tlog,transactions_file,completed,available):
        self.transaction_semaphore.acquire()
        if not completed and available:
            self.transaction_information[tlog["buyer"]] = tlog
        else:
            del self.transaction_information[tlog["buyer"]]
        with open(transactions_file,"w") as transact:
            json.dump(self.transaction_information,transact)
        self.transaction_semaphore.release()

def exit_handler():
    os.remove("seller_information.json")
    os.remove("transactions.json")

atexit.register(exit_handler)