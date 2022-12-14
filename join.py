from peer import Peer
import Pyro5
import random
import sys
from threading import Thread
import time

def get_peers():
    n_peers = int(sys.argv[2])
    n_items = 5
    n_traders = 2
    product_time = 3
    # max_neighbors = 3
    # hopcount = 3
    roles = ['buyer', 'seller']
    products = ['fish', 'salt', 'boar']
    ns_name = sys.argv[1]
    peers = []
    with_cache = True
    if sys.argv[3] == "true":
        fault_tolerance_heartbeat = True
        heartbeat_timeout = int(sys.argv[4])
    elif sys.argv[3] == "false":
        fault_tolerance_heartbeat = False
        heartbeat_timeout = 0
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
    peers.append(Peer(id, n_peers-2,role, n_items, product_time, products, ns_name, n_traders, with_cache,fault_tolerance_heartbeat,heartbeat_timeout))

    # ensures at least 1 buyer
    role = 'buyer'
    id = role + str(n_peers-1)
    peers.append(Peer(id, n_peers-1, role, n_items, product_time, products, ns_name, n_traders, with_cache,fault_tolerance_heartbeat,heartbeat_timeout))

    # add n_peers-2 buyers and sellers
    for i in range(n_peers - 2):
        # random assignment of roles
        role = roles[random.randint(0,len(roles) - 1)]
        id = role + str(i)
        peer = Peer(id, i, role, n_items, product_time, products, ns_name, n_traders, with_cache,fault_tolerance_heartbeat,heartbeat_timeout)
        peers.append(peer)

    peers.append(Peer('server9', -1, 'server', n_items, product_time, products, ns_name, n_traders, with_cache, fault_tolerance_heartbeat, heartbeat_timeout))

    return peers


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Incorrect number of arguments, the correct command is python3 join.py localhost number_of_arguments")
        sys.exit()
    peers = get_peers()

    time.sleep(2)
    try:
        for person in peers:
            person.start()
    except KeyboardInterrupt:
        sys.exit()






