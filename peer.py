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

        # for trader
        self.seller_information = {}
        self.product_information = {}
        self.transaction_information = {}
        self.storage_semaphore = BoundedSemaphore(1)
        self.transaction_semaphore = BoundedSemaphore(1)

        self.sendWon = False
        self.recvWon = False
        self.recvOK = False
        self.won_sem = BoundedSemaphore(1)

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

            # Select at most max_neighbors random neighbors from the neighbor list
            # while making sure that the selected neighbor also has at most max_neighbors neighbors
            for i in range(0, len(neighbor_list)):
                # if self.get_neighbor_len() >= self.max_neighbors:
                #     break
                # random_neighbor_id = neighbor_list[random.randint(0, len(neighbor_list)-1)]
                random_neighbor_id = neighbor_list[i]
                self.neighbors[random_neighbor_id] = self.ns.lookup(random_neighbor_id)

                with Pyro5.api.Proxy(self.neighbors[random_neighbor_id]) as neighbor:
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
                    print(datetime.datetime.now(), self.id, "joins to buy ", self.product_name)
                else:
                    print(datetime.datetime.now(), self.id, "joins to sell ", self.product_name)

                # Peer starts listening for requests
                self.executor.submit(daemon.requestLoop)
                time.sleep(1)

                # Create a neighbor list and assign neighbors to the peer
                self.get_random_neighbors()

                if self.bully_id <= 2:
                    # for neighbor_name in self.neighbors:
                    #     with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                    self.startElection()
                
                # # Buyer starts searching for a product
                # while True and self.role == "buyer":

                #     # Begin searching in a depth-first manner
                #     for neighbor_name in self.neighbors:
                #         with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:

                #             # Buyer forms the first element of the search path
                #             search_path = [self.id]
                #             print(datetime.datetime.now() , self.id, "searching for ", self.product_name, " in ", neighbor_name)
                #             neighbor.lookup(self.id, self.product_name, self.hopcount, search_path)

                #     # Lock the seller list to maintain consistency
                #     with self.seller_list_lock:

                #         print(datetime.datetime.now(), "seller list for ", self.id , "is: ", self.seller_list)
                        
                #         # Select a random seller
                #         if self.seller_list:
                #             random_seller = self.seller_list[random.randint(0, len(self.seller_list)-1)]

                #             # Buy a product from the selected seller
                #             with Pyro5.client.Proxy(self.neighbors[random_seller]) as seller:
                #                 # Claim ownership of the seller proxy since each Pyro proxy is a thread
                #                 seller._pyroClaimOwnership()
                #                 seller.buy(self.id)
                #         else:
                #             print(datetime.datetime.now(), "no sellers found within hop limit for ", self.id)
                        
                #         # Clear the seller list and assign a random product to the buyer
                #         self.seller_list = []
                #         self.product_name = self.products[random.randint(0, len(self.products)-1)]

                #     time.sleep(1)

                # Pausing helps seller to get registered with nameserver
                while True:
                    time.sleep(1)

        except Exception as e:
            print("Exception in main", e)
                            
    @Pyro5.server.expose
    def sendOutMessage(self):
        print(datetime.datetime.now(),"Dear buyers and sellers, my bully id is ",self.bully_id,"and I am vacating the coordinator position")
        self.recvWon = False
        self.recvOK = False
        self.sendWon = False
        self.trader= []
        self.prev_role = self.role
        self.role = self.prev_role
        self.prev_role = ""
        self.won_sem.release()
        for neighbor_name in self.neighbors:
            with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                neighbor.election_message("Out of Office",{"bully_id":self.bully_id,"id":self.id,"status":1})


    @Pyro5.server.expose
    def sendWonMessage(self):
        print(datetime.datetime.now(),"Dear buyers and sellers, my bully id is ",self.bully_id,"and I am the new coordinator")
        self.recvWon = True
        self.trader.append({"bully_id":self.bully_id,"id":self.id,"status":1})
        self.prev_role = self.role
        self.role = "Trader"
        self.won_sem.release()
        for neighbor_name in self.neighbors:
            if "seller" in neighbor_name:
                print("sending to my neighbor: ",neighbor_name)
                with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                    neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id,"status":1})
                    # try:
                    #     self.executor.submit(neighbor.election_message, "I Won",{"bully_id":self.bully_id,"id":self.id,"status":1})
                    # except Exception as e:
                    #     print(datetime.datetime.now(), "Exception in sending I won to sellers", e)
                # with Pyro5.client.Proxy(self.neighbors[neighbor_name]) as neighbor:
                #     # Claim ownership of the seller proxy since each Pyro proxy is a thread
                #     neighbor._pyroClaimOwnership()
                #     # seller.buy(self.id)
                #     neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id,"status":1})
        for neighbor_name in self.neighbors:
            if "buyer" in neighbor_name:
                print("sending to my neighbor: ",neighbor_name)
                with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                    neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id,"status":1})
                    # try:
                    #     self.executor.submit(neighbor.election_message, "I Won",{"bully_id":self.bully_id,"id":self.id,"status":1})
                    # except Exception as e:
                    #     print(datetime.datetime.now(), "Exception in sending I won to buyers", e)
                # with Pyro5.client.Proxy(self.neighbors[neighbor_name]) as neighbor:
                #     # Claim ownership of the seller proxy since each Pyro proxy is a thread
                #     neighbor._pyroClaimOwnership()
                #     # seller.buy(self.id)
                #     neighbor.election_message("I Won",{"bully_id":self.bully_id,"id":self.id,"status":1})

        # retire_chance = np.random.choice([i for i in range(1,21)],1)[0]
        # if retire_chance <= 7:
        #     self.won_sem.acquire()
        #     self.sendOutMessage()
        # else:
        print("Begin Trading")
        # self.startTrading()
            # for neighbor_name in self.neighbors:
            #     with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
            #         neighbor.election_message("I Won",{"peer_id":self.peer_id,"id":self.id,"status":1})

    @Pyro5.server.expose
    def election_message(self,message,neighbor):
        if message == "Election":
            if self.recvOK or self.recvWon:
                with Pyro5.api.Proxy(self.neighbors[neighbor["id"]]) as neighbor_x:
                    neighbor_x.election_message("OK",{"bully_id":self.bully_id,"id":self.id,"status":1})
            else:
                with Pyro5.api.Proxy(self.neighbors[neighbor["id"]]) as neighbor_x:
                    neighbor_x.election_message("OK",{"bully_id":self.bully_id,"id":self.id,"status":1})
                neighbor_ids = [int(x[-1]) for x in self.neighbors]
                neighbor_ids = np.array(neighbor_ids)
                greater_x = len(neighbor_ids[neighbor_ids > self.bully_id])
                if greater_x > 0:
                    self.recvWon = False
                    self.recvOK = False
                    for ng in self.neighbors:
                        if int(ng[-1]) > self.bully_id:
                            if self.trader != [] and int(ng[-1]) == self.trader[0]["bully_id"]:
                                continue
                            else:
                                with Pyro5.api.Proxy(self.neighbors[ng]) as neighbor:
                                    neighbor.election_message("Election",{"bully_id":self.bully_id,"id":self.id,"status":1})
                    time.sleep(2)
                    self.won_sem.acquire()
                    if self.recvOK == False and self.recvWon == False:
                        self.sendWon = True
                        self.sendWonMessage()
                    else:
                        self.won_sem.release()

                else:
                    self.won_sem.acquire()
                    if self.sendWon == False:
                        self.sendWon = True
                        self.sendWonMessage()
                    else:
                        self.won_sem.release()
        elif message == "OK":
            self.recvOK = True
        elif message == "I Won":
            print(datetime.datetime.now(),"Peer ",self.id,": Election Won Message received")
            self.won_sem.acquire()
            self.recvWon = True
            self.won_sem.release()
            self.trader.append(neighbor)
            time.sleep(2)
            print("Begin Trading for ",self.id)
            self.executor.submit(self.startTrading)
        # # TODO: add a fourth message saying "out of office" to restart the election process
        elif message == "Out of Office":
            print(datetime.datetime.now(),"Peer ",self.id,": Out of Office message received")
            self.won_sem.acquire()
            self.recvWon = False
            self.recvOK = False
            self.won_sem.release()
            self.trader = []
            # time.sleep(2)
            if self.bully_id <= 2:
                print("Peer ",self.id,": starting the election")
                self.startElection()

    @Pyro5.server.expose
    def startTrading(self):
        if self.role == "seller":
            # with self.trader[0]['id']
            print(datetime.datetime.now(),self.id," is registering its market for ",self.product_name)
            with Pyro5.api.Proxy(self.neighbors[self.trader[0]['id']]) as neighbor:
                neighbor.register_products({"seller":{"bully_id":self.bully_id,"id":self.id,"status":1},"product_name": self.product_name,"product_count":self.product_count})
        
        elif self.role == "buyer":
            #TODO: remove the hardcoded 6 value
            time.sleep(8.5+self.bully_id/6.0)
            while True:
                desired_item = self.products[random.randint(0, len(self.products)-1)]
                ## TODO
                ## TODO: add item count
                print(datetime.datetime.now(),self.id," is looking to buy ",desired_item)
                with Pyro5.api.Proxy(self.neighbors[self.trader[0]['id']]) as neighbor:
                    neighbor.trading_lookup({"bully_id":self.bully_id,"id":self.id,"status":1},desired_item)
                time.sleep(12)
        elif self.role == "Trader":
            pass

    @Pyro5.server.expose
    def register_products(self,seller_info):
        self.storage_semaphore.acquire()
        peer_id = seller_info["seller"]["id"]
        self.seller_information[peer_id] = seller_info

        pname = seller_info["product_name"]
        if pname in self.product_information:
            self.product_information[pname] += seller_info["product_count"]
        else:
            self.product_information[pname] = seller_info["product_count"]
        with open("seller_information.json","w") as sell:
            json.dump(self.seller_information,sell)
        self.storage_semaphore.release()
    
    @Pyro5.server.expose
    def trading_lookup(self,buyer_info,item):
        ##TODO

        print(datetime.datetime.now(),"Trader ",self.id," received request from buyer ",buyer_info["id"], "for product ",item)
        sellers = []
        transactions_file = "transactions.json"
        tlog = {"buyer":buyer_info["id"],"seller":"_","product":item,"completed":False}
        ## TODO: define put log
        self.put_log(tlog,transactions_file,False)
        for peer_id in self.seller_information.keys():
            if self.seller_information[peer_id]["product_name"] == item:
                sellers.append(self.seller_information[peer_id])

        if len(sellers)>0:
            # select the first available
            seller = sellers[0]
            print(seller)
            seller_peer_id = seller["seller"]["id"]
            self.seller_information[seller_peer_id]["product_count"] -= 1
            with open("seller_information.json","w") as sell:
                json.dump(self.seller_information,sell)
            tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"completed":False}
            self.put_log(tlog,transactions_file,False)
            
            
            # seller's information should update
            with Pyro5.api.Proxy(self.neighbors[seller_peer_id]) as neighbor:
                    neighbor.transaction(item,buyer_info["id"],seller_peer_id,self.id,False)

            tlog = {"buyer":buyer_info["id"],"seller":seller_peer_id,"product":item,"completed":True}
            self.put_log(tlog,transactions_file,True)
            # buyer should let know the success
            with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                    neighbor.transaction(item,buyer_info["id"],seller_peer_id,self.id,True)
        else:
            with Pyro5.api.Proxy(self.neighbors[buyer_info["id"]]) as neighbor:
                    neighbor.transaction(item,buyer_info["id"],"",self.id,False)

    @Pyro5.server.expose
    def transaction(self,product_name,buyer_id,seller_id,trader_id,buyer_success):
        if self.role == "seller" and self.product_name == product_name:
            print(datetime.datetime.now(),self.id," received request from trader ",trader_id," for item ",product_name)
            self.product_count -= 1
            if self.product_count == 0:
                self.product_name = self.products[random.randint(0, len(self.products)-1)]
                self.product_count = 3
                with Pyro5.api.Proxy(self.neighbors[self.trader[0]['id']]) as neighbor:
                    neighbor.register_products({"seller":{"bully_id":self.bully_id,"id":self.id,"status":1},"product_name": self.product_name,"product_count":self.product_count})

        elif self.role == "buyer":
            if buyer_success:
                print(datetime.datetime.now(),self.id," has got the item ",product_name)
            else:
                print(datetime.datetime.now(),self.id," could not find any seller for the item ",product_name)

    @Pyro5.server.expose
    def put_log(self,tlog,transactions_file,completed):
        self.transaction_semaphore.acquire()
        # if tlog["buyer"] not in self.transaction_information.keys():
        if not completed:
            self.transaction_information[tlog["buyer"]] = tlog
        else:
            del self.transaction_information[tlog["buyer"]]
        with open(transactions_file,"w") as transact:
            json.dump(self.transaction_information,transact)
        self.transaction_semaphore.release()
        
    @Pyro5.server.expose
    def startElection(self):
        neighbor_ids = [int(x[-1]) for x in self.neighbors]
        neighbor_ids = np.array(neighbor_ids)
        greater_x = len(neighbor_ids[neighbor_ids > self.bully_id])
        if greater_x > 0:
            self.recvWon = False
            self.recvOK = False
            for ng in self.neighbors:
                if int(ng[-1]) > self.bully_id:
                    if self.trader != [] and int(ng[-1]) == self.trader[0]["bully_id"]:
                        continue
                    else:
                        with Pyro5.api.Proxy(self.neighbors[ng]) as neighbor:
                            neighbor.election_message("Election",{"bully_id":self.bully_id,"id":self.id,"status":1})
            time.sleep(2)
            self.won_sem.acquire()
            if self.recvOK == False and self.recvWon == False:
                self.sendWon = True
                self.sendWonMessage()
            else:
                self.won_sem.release()

        else:
            self.won_sem.acquire()
            self.sendWon = True
            self.sendWonMessage()
        # for neighbor_name in self.neighbors:

    @Pyro5.server.expose
    def lookup(self, bID, product_name, hopcount, search_path):
        """
        Lookup a product in the peer's neighbor list
        :param bID: The id of the buyer
        :param product_name: The name of the product to lookup
        :param hopcount: The number of hops left. Set lower than the maximum distance between peers in the network
        :param search_path: The path of the search
        :return: nothing
        """
        # this procedure, executed initially by buyer processes then recursively 
        # between directly connected peer processes in the network
        
        last_peer = search_path[-1]
        hopcount -= 1

        # If hopcount is negative it means that the search has reached the maximum distance
        if hopcount < 0:
            return

        try:
            # If a seller is found with the product, add it to the seller list
            if self.role == "seller" and self.product_name == product_name and self.product_count >= 0:
                print(datetime.datetime.now(), "seller found with ID: ", self.id)
                # inserting seller id at the front of the search path for easier reply
                search_path.insert(0, self.id)
                self.executor.submit(self.reply, self.id, search_path)

            else:
                # Else search the neighbor list of the current peer
                for neighbor_name in self.neighbors:

                    # Make sure that the search does not go back to the peer that sent the request
                    if neighbor_name != last_peer:
                        with Pyro5.api.Proxy(self.neighbors[neighbor_name]) as neighbor:
                            if self.id not in search_path:
                                search_path.append(self.id)
                            self.executor.submit(neighbor.lookup, bID, product_name, hopcount, search_path)

        except Exception as e:
            print(datetime.datetime.now(), "Exception in lookup", e)
            return

    @Pyro5.server.expose
    def buy(self, peer_id):
        """
        Buy a product from the seller
        :param peer_id: The id of the buyer
        :return: nothing
        """

        try:
            # Lock the product count to maintain consistency during concurrent buy requests
            with self.product_count_lock:
                if self.product_count > 0:

                    self.product_count -= 1
                    print("*******\n", datetime.datetime.now(), peer_id, "bought", self.product_name, "from", self.id, self.product_count, "remain now", "\n*******")
                    return True
            
                # if self.product_count == 0, pick random item to sell
                else:
                    print(datetime.datetime.now(), peer_id, "failed to buy", self.product_name, "from", self.id, "no more items")
                    self.product_name = self.products[random.randint(0, len(self.products) - 1)]
                    self.product_count = self.n
                    print(datetime.datetime.now(), self.id , "now selling", self.product_name)
                    return False

        except Exception as e:
            print(datetime.datetime.now(), "Exception in buy", e)
            return

    @Pyro5.server.expose
    def reply(self, peer_id, reply_path):
        """
        Build the seller list for the buyer
        :param peer_id: The id of the seller
        :param reply_path: The complete path of the reply
        :return: nothing
        """

        try:
            # Only 1 peer id in reply_path which is the seller
            if reply_path and len(reply_path) == 1:
                print(datetime.datetime.now(), "Seller", peer_id, "responded to buyer", self.id)

                # Adding seller to the list of sellers
                with self.seller_list_lock:
                    if reply_path[0] not in self.seller_list:
                        self.seller_list.extend(reply_path)

            # If more than 1 peer id in reply_path, continue backward traversal
            elif reply_path and len(reply_path) > 1:
                intermediate_peer = reply_path.pop()
                with Pyro5.api.Proxy("PYRONAME:" + intermediate_peer) as neighbor:
                    neighbor.reply(peer_id, reply_path)

        except Exception as e:
            print(datetime.datetime.now(), "Exception in reply", e)
            return
