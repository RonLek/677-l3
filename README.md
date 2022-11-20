# COMPSCI 687 Lab 2

This repository contains the source code and the implementation of the Lab 2 Problem statement, which is a part of COMPSCI 677: Distributed and Operating Systems course of UMass Amherst Fall 2022.

Following is a brief excerpt of the problem statement:

> At the very beginning, the bazaar served the Gauls well. However, with the growing popularity of the bazaar, there were too many whispers that got lost in the din. After a town meeting, Vitalstatix, the village chief, decreed that henceforth the bazaar will adopt a trading post model. The Guals will use a leader election algorithm to elect a trader, who will be in charge of the trading post. All sellers will deposit their goods when the market opens with the trader. Buyers will buy from the trader and the trader will pay the seller after each sale (after deducting a suitable commission).

This implementation is done in Python3, in particular python3.7.15.

The main library used to implement this distributed setting is `Pyro5` which is a simple library equipped with functionalities to support distributed programming and working over remote objects and their communication.

## Instructions to run the code

The main command used to run this code is

```bash
python3 join.py localhost <number_of_peers>
```

In this command line argument `<number_of_peers>` refers to the number of peers to be included in this bazaar. This file will generate two intermediate files `sellers_information.json` and `transactions.json` which helps in storing the state of the trader at every transaction and seller information change for reliable working of this distributed bazaar. These files are transient and will be deleted by itself at the end of every program run.