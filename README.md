# COMPSCI 687 Lab 3

This repository contains the source code and the implementation of the Lab 3 Problem statement, which is a part of COMPSCI 677: Distributed and Operating Systems course of UMass Amherst Fall 2022.

Following is a brief excerpt of the problem statement:

> In the new market model, there will be one trader at each post. They shall use a common
warehouse where all items for sale are stored. Any item that is sold is shipped from the
warehouse, back to the trader, and handed off to the buyer. New items that are offered for sale
are added to the warehouse. Any trader can take buy or sell requests for any item. Sellers can
deposit their goods with any trader. Buyers can buy from any trader. For this assignment, you
don’t need to worry about buyer budgets or making sure sellers get paid — at this point, the
Gauls have attained a post-scarcity economy and don’t care much about money.

This implementation is done in Python3, in particular python3.8.

The main library used to implement this distributed setting is `Pyro5` which is a simple library equipped with functionalities to support distributed programming and working over remote objects and their communication.

## Running the code

The main command used to run this code is

```bash
python3 join.py localhost <number_of_peers> [fault_tolerance_toggle] [timeout_in_sec]
```

In this command line argument `<number_of_peers>` refers to the number of peers to be included in this bazaar. In addition the penultimate argument refers to toggling the fault tolerance situation in the bazaar. The input for this argument is "true"/"false". The last argument refers to the timeout value (in seconds) needed to fail one of the traders, should `fault_tolerance` flag is "true". In case of "false", this value should be set to 0.

During its runtime, the program will generate the following files:

- sellers_information.json: The data warehouse where product information is maintained.
- transactions_trader_<trader_id>.json: Multiple files (one per trader) which helps in storing the state of the trader at every transaction and seller information change.
- server_outputs.txt: File recording the output logs for the server.
- trader_<trader_id>.txt: Multiple files (one per trader) used to record the output logs for each trader.

An example command to run the program with cache and fault-tolerance is as follows:

```bash
cd asterix and multi-trader
python3 join.py localhost 6 true 15
```

## Development

In case you intend to run the code repeatedly, the seller_information.json and transactions_trader_*.json files need to be deleted before running the code again. This is because the code uses the information from these files and if the files are not deleted, the code will not work as expected.

Python's [atexit](http://docs.python.org/library/atexit.html) library has been used to implement auto-deletion of these files after every exit from the program (except fatal internal errors).

Uncomment the last line in `peer.py` to enable the auto-deletion of these files.
