from peer import Peer
from threading import Thread
import Pyro5
import random
import sys
import time

def get_peers():
    n_peers = int(sys.argv[2])
    n_items = 5
    max_neighbors = 3
    hopcount = 3
    roles = ['buyer', 'seller']
    products = ['fish', 'salt', 'boar']
    ns_name = sys.argv[1]
    peers = []
    
    # Starts name server
    try:
        ns = Pyro5.api.locate_ns()
    except Exception:
        print("No server found, start one")
        Thread(target=Pyro5.nameserver.start_ns_loop, kwargs={"host": ns_name}).start()
        # wait for the nameserver to be up
        time.sleep(2)

    # ensures at least 1 seller
    role = 'seller'
    id = role + str(n_peers-2)
    peers.append(Peer(id, n_peers-2,role, n_items, products, ns_name, max_neighbors, hopcount))

    # ensures at least 1 buyer
    role = 'buyer'
    id = role + str(n_peers-1)
    peers.append(Peer(id, n_peers-1, role, n_items, products, ns_name, max_neighbors, hopcount))

    # add n_peers-2 buyers and sellers
    for i in range(n_peers - 2):
        # random assignment of roles
        role = roles[random.randint(0,len(roles) - 1)]
        id = role + str(i)
        peer = Peer(id, i, role, n_items, products, ns_name, max_neighbors, hopcount)
        peers.append(peer)

    return peers


if __name__ == "__main__":

    peers = get_peers()

    time.sleep(2)
    try:
        for person in peers:
            person.start()
    except KeyboardInterrupt:
        sys.exit()






