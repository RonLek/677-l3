# class to implement a peer - can be a buyer or a seller
import atexit
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import numpy as np
import os.path
import Pyro5.server
import Pyro5.api
import random
import re
import glob
from threading import BoundedSemaphore
from multiprocessing import Process
import time
class Peer(Process):
    """
    The Peer class represents a buyer or a seller within the P2P network.
    It has the following methods:
    1. get_random_neighbors - Create a neighbor list and assign neighbors to the peer
    2. connect_neighbors - Select at most max_neighbors random neighbors from the neighbor list and connect to them
    3. add_neighbor - Add a neighbor to the peer's neighbor list
    4. get_neighbor_len - Get the number of neighbors of the peer
    5. get_nameserver - Get the nameserver proxy
    6. run - Starts the market simulation
    """

    def __init__(self, id, bully_id, role, product_count, product_time, products, hostname, n_traders, with_cache,fault_tolerance_heartbeat,heartbeat_timeout):
        """
        Construct a new 'Peer' object.

        :param id: The id of the peer
        :param bully_id: The bully id of the peer
        :param role: The role of the peer
        :param product_count: The maximum number of products the peer can sell (if role is seller)
        :param products: The list of products that a peer can buy or sell
        :param hostname: The hostname of the peer
        :return: returns nothing
        """
        Process.__init__(self)
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
        self.product_time = product_time
        self.ns = self.get_nameserver(hostname)
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.with_cache = with_cache
        # to store previous role when elected to trader
        self.prev_role = ""

        self.price = random.randint(1,10)
        # for trader
        self.seller_information = {}
        self.transaction_information = {}
        self.storage_semaphore = BoundedSemaphore(1)
        self.transaction_semaphore = BoundedSemaphore(1)
        self.trading_list_semaphore = BoundedSemaphore(1)
        self.n_traders = n_traders

        self.heartbeat_status = True
        self.fault_tolerance_heartbeat = fault_tolerance_heartbeat
        self.heartbeat_timeout = heartbeat_timeout

        # for seller
        self.seller_amount = 0
        
        # for failure condition on buyers
        self.buy_request_done = False
        self.buy_request_semaphore = BoundedSemaphore(1)
        self.fail_sem = BoundedSemaphore(1)
        self.sendWon = False
        self.recvWon = False
        self.recvOK = False
        self.won_sem = BoundedSemaphore(1)
        self.product_sem = BoundedSemaphore(1)

        # for multicast lamport clocks
        self.clock_sem = BoundedSemaphore(1)
        self.clock = 0 + int(self.id[-1]) / 10.0
        self.buyer_list = [] # only for seller

    def get_neighbors(self):
        """
        Create a neighbor list and assign neighbors to the peer
        :return: returns nothing
        """
        neighbor_list = []
        ns_dict = self.ns.list()
        re_pattern = "seller[0-9]+|buyer[0-9]+|server9"

        # Build a list of neighbors from the nameserver excluding the peer itself
        for id in ns_dict:
            if "NameServer" not in id and self.id != id and re.match(re_pattern, id):
                neighbor_list.append(id)
        self.connect_neighbors(neighbor_list)

        neighbor_list.clear()

    def connect_neighbors(self, neighbor_list):
        """
        Select all peers as neighbors and connect to them for fully connected network
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
                elif self.role == "seller":
                    print(datetime.datetime.now(), self.id, "joins to sell ", self.product_name, " with bully id ", self.bully_id)
                else:
                    print(datetime.datetime.now(), self.id, "joins as server process ")

                # Peer starts listening for requests
                self.executor.submit(daemon.requestLoop)
                time.sleep(1)

                # Create a neighbor list and assign neighbors to the peer
                self.get_neighbors()

                # Peer 0 elects nt traders
                # TODO: Handle 
                traders = []
                if self.id[-1] == "0":
                    while len(traders) != self.n_traders:
                        self.startElection()
                        
                        # get all traders
                        for neighbor_name in self.neighbors:
                            with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                                if neighbor.isTrader() and neighbor_name not in traders:
                                    traders.append(neighbor_name)


                        if self.role == "trader" and self.id not in traders:
                            traders.append(self.id)

                    print(datetime.datetime.now(), "traders = ", traders)

                    for neighbor_name in self.neighbors:
                        with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                            neighbor.setTrader(traders)
                    self.setTrader(traders)
                    
                    if self.fault_tolerance_heartbeat:
                        for i,trader in enumerate(self.trader):
                            if self.id != trader:
                                with Pyro5.api.Proxy(self.neighbors[trader]) as neighbor:
                                    neighbor.startTrading(i)
                        if self.role == "trader":
                            # traders.append(self.id)
                            self.startTrading(1)

  

                    while True:
                        # Peer 0 starts the market simulation
                        # Register seller products with the warehouse
                        print(datetime.datetime.now(), "sellers register products with trader")
                        for neighbor_name in self.neighbors:
                            print("before seller", neighbor_name)
                            # print("neighbors", self.neighbors)
                            with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                                if "seller" in neighbor_name and not neighbor.isRetire():
                                    neighbor.startSellerTrading()

                        # Register self if seller
                        # Case when peer 0 is a trader handled in startSellerTrading
                        if "seller" in self.id:
                            self.startSellerTrading()

                        # Start buyer threads for trading
                        print(datetime.datetime.now(), "buyers start trading on threads")
                        for neighbor_name in self.neighbors:
                            with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                                if "buyer" in neighbor_name and not neighbor.isTrader() and not neighbor.isRetire():
                                    neighbor.sendBuyRequest()
                                    time.sleep(1)

                        # Buy for self
                        if "buyer" in self.id and not self.isTrader() and not self.isRetire():
                            self.sendBuyRequest()
                            time.sleep(1)
                                
                while True:
                    time.sleep(1)

        except Exception as e:
            print(datetime.datetime.now(), "Exception in main", e.with_traceback())

    @Pyro5.server.expose
    def sendBuyRequest(self):
        # select a random trader
        trader = random.choice(self.trader)
        with Pyro5.api.Proxy(self.neighbors[trader]) as neighbor:
            neighbor.trading_lookup(self.tradingMessage(), self.product_name, self.product_count)

    @Pyro5.server.expose
    def setTrader(self, traders):
        self.trader = traders

    @Pyro5.server.expose
    def election_message(self,message,neighbor):
        """
        Receive election message from a neighbor
        :param message: The message received
        :param neighbor: The neighbor that sent the message
        :return: nothing
        """

        print(datetime.datetime.now(), self.id, "received message", message, " from ", neighbor)
        # Acquire and release semaphore to ensure only one thread can access the critical section at a time
        self.clock_sem.acquire()
        self.adjustClockValue(neighbor["clock"])
        self.clock_sem.release()

        if self.role == "trader" or self.role == "server":
            pass

        if message == "Election":
            # Send back "OK" if either of recvOK or recvWon is true
            if self.recvOK or self.recvWon:
                with Pyro5.api.Proxy(self.neighbors[neighbor["id"]]) as neighbor_x:
                    neighbor_x.adjustClockValue(self.clock)
                    neighbor_x.election_message("OK",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
            else:
                # Send back "OK" to previous neighbor and "Election" to next higher neighbor
                with Pyro5.api.Proxy(self.neighbors[neighbor["id"]]) as neighbor_x:
                    neighbor_x.adjustClockValue(self.clock)
                    neighbor_x.election_message("OK",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})

                # Find neighbors with higher bully id
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
                                    if self.sendWon == False and (self.recvOK == False or self.recvWon == False):
                                        # Message send event, increment clock value
                                        self.clock_sem.acquire()
                                        self.forwardClockValue()
                                        self.clock_sem.release()

                                        neighbor.election_message("Election",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})
                    time.sleep(2)
                    self.won_sem.acquire()
                    # If no OK or Won messages received, declare self as winner
                    if not self.recvOK and not self.recvWon:
                        self.sendWon = True
                        # Semaphore released in sendWonMessage
                        self.sendWonMessage()
                    else:
                        self.won_sem.release()

                # If no neighbors with higher bully id, become coordinator
                else:
                    self.won_sem.acquire()
                    if self.sendWon == False:
                        self.sendWon = True
                        self.sendWonMessage()
                    else:
                        self.won_sem.release()

        # If OK message received, set recvOK to true
        elif message == "OK":
            self.recvOK = True

        # If Won message received, set recvWon to true
        elif message == "I Won":
            print(datetime.datetime.now(),"peer ",self.id,": election won message received")
            self.won_sem.acquire()
            self.recvWon = True
            self.won_sem.release()
            # self.trader.clear() 
            self.trader.append(neighbor)

    @Pyro5.server.expose
    def isTrader(self):
        """
        Check if peer is a trader
        :return: True if peer is a trader, False otherwise
        """

        return self.role == "trader"
    @Pyro5.server.expose
    def isRetire(self):
        """
        Check if peer is a trader
        :return: True if peer is a trader, False otherwise
        """
        return self.role == "retire"

    @Pyro5.server.expose
    def isServer(self):
        """
        Check if peer is a server
        :return: True if peer is a server, False otherwise
        """

        return self.role == "server"

    @Pyro5.server.expose
    def startElection(self):
        """
        Start the election
        :return: nothing
        """
        print(datetime.datetime.now(),self.id," starting election")
        # time.sleep(2)
        
        # Sets default values for recvOK, recvWon, sendWon before starting election
        for x in self.neighbors:
            with Pyro5.api.Proxy(self.neighbors[x]) as neighbor:
                if not neighbor.isTrader() and not neighbor.isServer():
                    neighbor.setDefaultFlags()
        self.setDefaultFlags()
       
        # Find neighbors with higher bully id
        neighbor_bully_ids = {}
        for neighbor_id in self.neighbors:
            with Pyro5.api.Proxy(self.neighbors[neighbor_id]) as neighbor:
                neighbor_bully_ids[neighbor_id] = neighbor.get_bully_id()
                greater_bullies = [k for k, v in neighbor_bully_ids.items() if v > self.bully_id]
        if len(greater_bullies) > 0:
            self.recvWon = False
            self.recvOK = False
            for ng in greater_bullies:
                with Pyro5.api.Proxy(self.neighbors[ng]) as neighbor:
                    if self.recvOK == False and self.recvWon == False and neighbor.isTrader() == False:
                        # Message send event, increment clock value
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
                self.sendWonMessage()
            else:
                self.won_sem.release()
        # if no greater bully id is found, it is the coordinator
        else:
            self.won_sem.acquire()
            self.sendWon = True
            # semaphore is released in the sendWonMessage method
            self.sendWonMessage()

    @Pyro5.server.expose
    def setDefaultFlags(self):
        """
        Set default values for recvOK, recvWon, sendWon
        :return: nothing
        """
        self.recvOK = False
        self.recvWon = False
        self.sendWon = False
        self.bully_id = random.randint(0,200)

    @Pyro5.server.expose
    def startTrading(self,fail_one):
        """
        Start trading as a seller
        :return: nothing
        """
        print(datetime.datetime.now(),"Trading initiated by ",self.id)

        if self.role == "trader":
            other_trader = ""
            if self.id == self.trader[0]:
                other_trader = self.trader[1]
            else:
                other_trader = self.trader[0]
            # with Pyro5.api.Proxy(self.neighbors[other_trader]) as neighbor:
            #     print(other_trader)
            #     print(neighbor)
            if fail_one == 1:
                self.executor.submit(self.retire_with_time,self.heartbeat_timeout)
            self.executor.submit(self.ping_message, other_trader)
            # pass

    @Pyro5.server.expose
    def retire_with_time(self,ttl):
        time.sleep(ttl)
        print(self.id,"[Trader]","Retiring from the market")
        self.role = "retire"
    
    @Pyro5.server.expose
    def ping_message(self,neighbor_id):
        print(self.id,"[Trader]","inside ping message")
        while self.role == "trader":
            self.heartbeat = False
            self.executor.submit(self.ping_send,neighbor_id)
            # neighbor.ping_reply()
            time.sleep(10)
            print(self.id,"[Trader]","Heartbeat: ",self.heartbeat)
            if not self.heartbeat:
                break
        if self.role == "trader":
            print(self.id,"[Trader]","Dead other rader",neighbor_id)
            old_index_file = "transactions_trader_"+neighbor_id+".json"
            print(self.id,"[Trader]","Dead other trader old file: ",old_index_file)
            for neighbor_name in self.neighbors:
                with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor_1:
                    neighbor_1.removeTrader(neighbor_id)
            self.removeTrader(neighbor_id)
            print(self.id,"[Trader]","Other trader removal successful")
            with open(old_index_file,"w") as transact:
                print(self.id,"[Trader]","Entering pending transactions of other trader")
                pending_req = json.load(transact)
                if not pending_req:
                    for req in pending_req:
                        k,v  = req.items()[0]
                        print(self.id,"[Trader]",k,v)
                        # self.trading_unresolved_lookup(v)
        return
        

    @Pyro5.server.expose
    def removeTrader(self,neighbor_id):
        self.trader.remove(neighbor_id)

    @Pyro5.server.expose
    def ping_reply(self,neighbor_id):
        if not self.isRetire():
            print(self.id,"[Trader]","Received Ping from ",neighbor_id)
            return True
        else:
            return False

    @Pyro5.server.expose
    def ping_send(self,neighbor_id):
        print(self.id,"[Trader]","In Ping send")
        with Pyro5.api.Proxy(self.neighbors[neighbor_id]) as neighbor:
            print(self.id,"[Trader]","In Ping send, found neighbour")
            print(neighbor_id)
            print(neighbor)
            x = neighbor.ping_reply(self.id)
        print(self.id,"[Trader]","Ping Reply: ",x)
        self.heartbeat = x

    @Pyro5.server.expose
    def startSellerTrading(self):
        """
        Start trading as a seller
        :return: nothing
        """
        print(datetime.datetime.now(),"trading entered by ",self.id)

        if self.role == "seller":
            # Register seller products with the coordinator
            print(datetime.datetime.now(),self.id," is registering its market for ",self.product_name)
            print(datetime.datetime.now(),self.id," traders = ",self.trader)
            with Pyro5.api.Proxy(self.neighbors[random.choice(self.trader)]) as neighbor:
                neighbor.register_products({"seller":{"bully_id":self.bully_id,"id":self.id},"product_name": self.product_name,"product_count":self.product_count,"product_price":self.price,"seller_amount":self.seller_amount})
        
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

        # Send Won message to all neighbors
        for neighbor_name in self.neighbors:
                print(datetime.datetime.now(), "sending won message to neighbor: ",neighbor_name)
                with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:

                    # Message send event, increment clock value
                    self.clock_sem.acquire()
                    self.forwardClockValue()
                    self.clock_sem.release()

                    neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id, "clock":self.clock})

        print(datetime.datetime.now(), "coordinator notified all neighbors.")

    @Pyro5.server.expose
    def tradingMessage(self):
        """"
        Construct data structure for trading message
        :return: trading message
        """
        return {"bully_id":self.bully_id,"id":self.id, "clock":self.clock}

    @Pyro5.server.expose
    def get_bully_id(self):
        """
        Get bully id of the peer
        :return: bully id
        """
        return self.bully_id

    @Pyro5.server.expose
    def productName(self):
        """
        Get product name of the peer
        :return: product name
        """
        return self.product_name

    @Pyro5.server.expose
    def addBuyer(self, buyer_id):
        """
        Add buyer to the list of buyers for the seller
        :param buyer_id: buyer id
        :return: nothing
        """
        self.buyer_list.append(buyer_id)

    @Pyro5.server.expose
    def adjustClockValue(self, other):
        """
        Adjust clock value of the peer
        :param other: clock value of the sender
        :return: nothing
        """
        self.clock = max(int(self.clock), int(other)) + 1 + int(self.id[-1]) / 10.0

    @Pyro5.server.expose
    def forwardClockValue(self):
        """
        Forward clock value of the peer
        :return: nothing
        """
        self.clock += 1

    @Pyro5.server.expose
    def getClock(self):
        """
        Get clock value of the peer
        :return: clock value
        """
        return self.clock

    @Pyro5.server.expose
    def check_seller_in_cache(self, item, item_count):
        sellers = []
        found_seller = ''
        found = False
        for peer_id in self.seller_information.keys():
            if self.seller_information[peer_id]["product_name"] == item and self.id != peer_id:
                sellers.append(self.seller_information[peer_id])

        # If sellers are found, send a request
        if len(sellers)>0:
            found = True
            # Iterate over all sellers to find potential matches to the buyer requests
            for sl in sellers:
                sl_id = sl["seller"]["id"]
                if self.seller_information[sl_id]["product_count"] < item_count:
                    continue
                else:
                    found_seller = sl
                    break
        return found_seller, found

    @Pyro5.server.expose
    def trading_lookup(self,buyer_info,item,item_count):
        """
        Match sellers to buyers by product name
        :param buyer_info: buyer information
        :param item: product name
        :param item_count number of items to buy
        :return: nothing
        """
        self.fail_sem.acquire()

        if self.role == "trader":
            print(datetime.datetime.now(),"trader ",self.id," received request from buyer ",buyer_info["id"], "for product ",item,"("+str(item_count)+")")
            transactions_file = "transactions_trader_"+self.id+".json"
            # Save current incomplete transaction to a file for recovery
            tlog = {"buyer":buyer_info["id"],"seller":"_","product":item,"product_count":item_count,"completed":False}
            self.put_log(tlog,transactions_file,False,True)

            # Make the trader fail with a probability
            # retire_chance = np.random.choice([i for i in range(1,21)],1)[0]
            # if retire_chance <= 5:
            #     self.won_sem.acquire()
            #     print(datetime.datetime.now(),"trader ",self.id, " is vacating the coordinator position silently")
            #     self.recvWon = False
            #     self.recvOK = False
            #     self.sendWon = False
            #     self.trader= []
            #     self.role = self.prev_role
            #     self.prev_role = ""
            #     self.trader.clear()
            #     print(datetime.datetime.now(), "trader assumed new role: ", self.role)
            #     self.won_sem.release()
            #     self.fail_sem.release()
            #     return

            # Find sellers with the product
            print(self.seller_information)
            sl = ''
            found = False

            if self.with_cache:
                # Check if the seller is in the cache
                sl, found = self.check_seller_in_cache(item,item_count)

            print("cache before ", found, sl, self.seller_information)
            # If not found in cache, load state and check again, avoids underselling
            if not sl or not found:
                self.load_state()
                sl, found = self.check_seller_in_cache(item,item_count)
            
            print("cache after ", found, sl, self.seller_information)
            
            if found:
                if not sl:
                    # When no seller can fulfill the demand, simply reject the buyer request from trader
                    with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                            neighbor.transaction(item,buyer_info["id"],"",self.id,False,True,0,item_count)
                            self.put_log(tlog,transactions_file,True,False)
                            self.fail_sem.release()
                            return
                else:
                    
                    seller = sl
                    # print("[DEBUG] seller: ", seller, " chosen for transaction")
                    seller_peer_id = seller["seller"]["id"]
                    print("seller with peer id ", seller_peer_id, " chosen for transactions")
                    # Add seller to the transaction log along with buyers interested in buying from it
                    # Update the trader's seller information to update the selected seller's transaction and its amount
                    # and save it to saved transactions file
                    try:
                        self.seller_information[seller_peer_id]["product_count"] -= item_count
                        self.seller_information[seller_peer_id]["seller_amount"] += item_count*seller['product_price']
                        self.seller_information[seller_peer_id]["buyer_list"].append(buyer_info["id"])
                        print("seller information after buy", self.seller_information)
                        with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as seller_add:
                            seller_add.addBuyer(buyer_info["id"])
                        # self.storage_semaphore.acquire()
                        data = {}
                        with open("seller_information.json") as sell:
                            data = json.load(sell)
                        with open("seller_information.json", "w") as sell:
                            data[seller_peer_id]["product_count"] -= item_count
                            data[seller_peer_id]["seller_amount"] += item_count*seller['product_price']
                            data[seller_peer_id]["buyer_list"].append(buyer_info["id"])
                            print("data after buy", data)
                            json.dump(data,sell)
                        # self.storage_semaphore.release()
                        tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"product_count":item_count,"completed":False}
                        self.put_log(tlog,transactions_file,False,True)
                    except Exception as e:
                        print("[DEBUG] error in updating transaction information: ", e)
                    
                    # Choose a buyer and decrement the product count
                    with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as neighbor:
                            neighbor.transaction(item,buyer_info["id"], seller_peer_id,self.id,False,False,0,item_count)

                    tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"product_count":item_count,"completed":True}
                    self.put_log(tlog,transactions_file,True,True)

                    # Let buyer know that the transaction is complete
                    with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                            neighbor.transaction(item,buyer_info["id"], seller_peer_id,self.id,True,False,seller['product_price'],item_count)
            else:
                with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                        neighbor.transaction(item,buyer_info["id"],"",self.id,False,False,0,item_count)
                        self.put_log(tlog,transactions_file,True,False)

        self.fail_sem.release()

    @Pyro5.server.expose
    def trading_unresolved_lookup(self,tlog):
        """
        Match sellers to buyers by product name
        :param buyer_info: buyer information
        :param item: product name
        :param item_count number of items to buy
        :return: nothing
        """
        # self.fail_sem.acquire()

        if self.role == "trader":
            item = tlog["product"]
            item_count = tlog["product_count"]
            buyer_info_id = buyer_info_id

            transactions_file = "transactions_trader_"+self.id+".json"
            if tlog["seller"] == "_" and not tlog["completed"]:
                
                self.put_log(tlog,transactions_file,False,True)


                # Find sellers with the product
                print(self.seller_information)
                sl, found = self.check_seller_in_cache(item,item_count)

                # If not found in cache, load state and check again, avoids underselling
                if not sl or not found and self.with_cache:
                    self.load_state()
                    sl, found = self.check_seller_in_cache(item,item_count)
                
                if found:
                    if not sl:
                        # When no seller can fulfill the demand, simply reject the buyer request from trader
                        with Pyro5.api.Proxy(self.neighbors[buyer_info_id]) as neighbor:
                                neighbor.transaction(item,buyer_info_id,"",self.id,False,True,0,item_count)
                                self.put_log(tlog,transactions_file,True,False)
                                self.fail_sem.release()
                                return
                    else:
                        
                        seller = sl
                        # print("[DEBUG] seller: ", seller, " chosen for transaction")
                        seller_peer_id = seller["seller"]["id"]
                        print("seller with peer id ", seller_peer_id, " chosen for transactions")
                        # Add seller to the transaction log along with buyers interested in buying from it
                        # Update the trader's seller information to update the selected seller's transaction and its amount
                        # and save it to saved transactions file
                        try:
                            self.seller_information[seller_peer_id]["product_count"] -= item_count
                            self.seller_information[seller_peer_id]["seller_amount"] += item_count*seller['product_price']
                            self.seller_information[seller_peer_id]["buyer_list"].append(buyer_info_id)
                            with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as seller_add:
                                seller_add.addBuyer(buyer_info_id)
                            with open("seller_information.json","w") as sell:
                                data = json.load(sell)
                                data[seller_peer_id]["product_count"] -= item_count
                                data[seller_peer_id]["seller_amount"] += item_count*seller['product_price']
                                data[seller_peer_id]["buyer_list"].append(buyer_info_id)
                                json.dump(data,sell)
                            tlog = {"buyer":buyer_info_id,"seller":seller_peer_id,"product":item,"product_count":item_count,"completed":False}
                            self.put_log(tlog,transactions_file,False,True)
                        except Exception as e:
                            print("[DEBUG] error in updating transaction information: ", e)
                        
                        # Choose a buyer and decrement the product count
                        with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as neighbor:
                                neighbor.transaction(item,buyer_info_id, seller_peer_id,self.id,False,False,0,item_count)

                        tlog = {"buyer":buyer_info_id,"seller":seller_peer_id,"product":item,"product_count":item_count,"completed":True}
                        self.put_log(tlog,transactions_file,True,True)

                        # Let buyer know that the transaction is complete
                        with Pyro5.api.Proxy(self.neighbors[buyer_info_id]) as neighbor:
                                neighbor.transaction(item,buyer_info_id, seller_peer_id,self.id,True,False,seller['product_price'],item_count)
                else:
                    with Pyro5.api.Proxy(self.neighbors[buyer_info_id]) as neighbor:
                            neighbor.transaction(item,buyer_info_id,"",self.id,False,False,0,item_count)
                            self.put_log(tlog,transactions_file,True,False)
            elif tlog["seller"] != "_" and not tlog["completed"]:
                seller_peer_id = tlog["seller"]
                try:
                    # self.seller_information[seller_peer_id]["product_count"] -= item_count
                    # self.seller_information[seller_peer_id]["seller_amount"] += item_count*seller['product_price']
                    # self.seller_information[seller_peer_id]["buyer_list"].append(buyer_info["id"])
                    # with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as seller_add:
                    #     seller_add.addBuyer(buyer_info["id"])
                    # with open("seller_information.json","w") as sell:
                    #     json.dump(self.seller_information,sell)
                    # tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"product_count":item_count,"completed":False}
                    self.put_log(tlog,transactions_file,False,True)
                except Exception as e:
                    print("[DEBUG] error in updating transaction information: ", e)
                
                # Choose a buyer and decrement the product count
                with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as neighbor:
                        neighbor.transaction(item,buyer_info_id, seller_peer_id,self.id,False,False,0,item_count)

                tlog = {"buyer":buyer_info_id,"seller":seller_peer_id,"product":item,"product_count":item_count,"completed":True}
                self.put_log(tlog,transactions_file,True,True)

                # Let buyer know that the transaction is complete
                with Pyro5.api.Proxy(self.neighbors[buyer_info_id]) as neighbor:
                        neighbor.transaction(item,buyer_info_id, seller_peer_id,self.id,True,False,seller['product_price'],item_count)

    @Pyro5.server.expose
    def addBuyer(self, buyer_id):
        self.buyer_list.append(buyer_id)

    @Pyro5.server.expose
    def transaction(self,product_name,buyer_info_id,seller_id,trader_id,buyer_success,insufficient,sell_price,item_cnt):
        """
        Complete the transaction at the buyer and seller
        :param product_name: product name
        :param buyer_info: buyer information
        :param seller_id: seller id
        :param trader_id: trader id
        :param buyer_success: boolean indicating if the transaction was successful for the buyer
        :param insufficient: boolean indicating the buyer demand can't be satisfied by any of seller's resources currently
        :param sell_price: integer denoting the price of the item to be sold
        :param item_cnt: integer denoting the number of items to be sold
        :return: nothing
        """
        if self.role == "seller" and self.product_name == product_name:
            print(datetime.datetime.now(),self.id," received request from trader ",trader_id," for item ",product_name,"("+str(item_cnt)+")")
            buyer_clocks = {}
            for buyer in self.buyer_list:
                with Pyro5.api.Proxy(self.neighbors[buyer]) as neighbor:
                    buyer_clocks[buyer] = neighbor.getClock()

            # Choose the buyer with the highest clock value
            max_key = max(buyer_clocks, key=buyer_clocks.get)

            # If the max_key buyer is the one who initiated the transaction, complete the transaction
            if max_key == buyer_info_id:
                # self.product_count -= item_cnt
                self.seller_amount += item_cnt*self.price
                print(datetime.datetime.now(),self.id," sold ",item_cnt," ",product_name," to ",buyer_info_id," for ",item_cnt*self.price)
                print(datetime.datetime.now(),self.id," seller amount = ",self.seller_amount)
                # if self.product_count == 0:
                #     # self.product_name = self.products[random.randint(0, len(self.products)-1)]
                #     self.product_count = 3
                #     self.buyer_list.clear()
                #     with Pyro5.api.Proxy(self.neighbors[trader_id]) as neighbor:
                #         neighbor.register_products({"seller":{"bully_id":self.bully_id,"id":self.id},"product_name": self.product_name,"product_count":self.product_count,"product_price":self.price,"seller_amount":self.seller_amount})
        elif self.role == "buyer":
            if buyer_success:
                print("**********")
                print(datetime.datetime.now(),self.id," bought item ",product_name, " from seller ",seller_id," for price ",sell_price)
                print("**********")
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
        """
        Register the seller information
        :param seller_info: seller information
        :return: nothing
        """
        
        print("within register_products")
        print(seller_info)
        print("registering with, ", self.id)
        peer_id = seller_info["seller"]["id"]
        seller_info["buyer_list"] = []

        
        if peer_id in self.seller_information.keys():
            self.seller_information[peer_id]["product_count"] += seller_info["product_count"]
        else:
            self.seller_information[peer_id] = seller_info
        
        print("seller_information = ", self.seller_information)

        # update data in warehouse
        with Pyro5.api.Proxy(self.neighbors["server9"]) as neighbor:
            neighbor.register_products_with_warehouse(seller_info)

    @Pyro5.server.expose
    def register_products_with_warehouse(self, seller_info):
        peer_id = seller_info["seller"]["id"]
        data = {}
        if os.path.exists("seller_information.json"):
            self.storage_semaphore.acquire()
            with open("seller_information.json") as sell:
                data = json.load(sell)
                print("data existing = ",data)
            self.storage_semaphore.release()

        self.storage_semaphore.acquire()
        with open("seller_information.json","w") as sell:
            if peer_id in data.keys():
                data[peer_id]["product_count"] += seller_info["product_count"]
            else:
                data[peer_id] = seller_info

            json.dump(data,sell)
        self.storage_semaphore.release()
        print("data updated = ",data)
    
        with open("seller_information.json") as sell:
            print("data in file = ",json.load(sell))

    @Pyro5.server.expose
    def load_state(self):
        """
        Load the state of the peer
        :return: nothing
        """
        if os.path.exists("seller_information.json"):
            self.storage_semaphore.acquire()
            with open("seller_information.json") as sell:
                self.seller_information = json.load(sell)
            # if self.id in self.seller_information.keys():
            self.storage_semaphore.release()

    @Pyro5.server.expose
    def put_log(self,tlog,transactions_file,completed,available):
        """
        Put the transaction log
        :param tlog: transaction log
        :param transactions_file: transaction file
        :param completed: boolean indicating if the transaction is completed
        :param available: boolean indicating if the previous transaction was available as logged
        :return: nothing
        """
        self.transaction_semaphore.acquire()
        if not completed and available:
            self.transaction_information[tlog["buyer"]] = tlog
        else:
            del self.transaction_information[tlog["buyer"]]
        with open(transactions_file,"w") as transact:
            json.dump(self.transaction_information,transact)
        self.transaction_semaphore.release()

def exit_handler():
    """
    Exit handler
    :return: nothing
    """
    os.remove("seller_information.json")
    for f in glob.glob("transactions_*.json"):
        os.remove(f)
    # os.remove("transactions_trader_0.json")
    # os.remove("transactions_trader_1.json")

# Uncomment while debugging to remove the seller and transaction files
atexit.register(exit_handler)
