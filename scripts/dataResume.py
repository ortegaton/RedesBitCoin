import pickle
from igraph import *
from multiprocess import Process, Manager
import time
import cProfile
import json
import numpy as np


def load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return Graph.DictList(gl['vertices'].values(),gl['edges'],directed=True)

def has_tag(tag):
    return lambda v: 'tag' in v.attributes() and v['tag'] == [tag]

def raw_load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return gl

transactions = {}
addresses = {}
transactions_blocks = {}
addresses_blocks = {}
times = {}

current_block = 481935
while current_block <= 482135:
    print current_block
    g = load_graph('{}-{}.p'.format(current_block, current_block+200))
    blocks = np.unique([tx['block'] for tx in g.vs.select(type_eq = "Transaction")])
    for block in blocks:
        block_transactions  = g.vs.select(type_eq = "Transaction", block_eq = block)
        block_addresses = g.vs[sum([g.neighbors(tx) for tx in block_transactions],[])]['name']
        transactions_blocks[block] = len(block_transactions)
        addresses_blocks[block] = len(np.unique(block_addresses))
        times[block] = block_transactions[0]['timestamp']
    graph_transactions = g.vs.select(type = "Transaction")
    graph_addresses = g.vs.select(type = "Address")['name']
    transactions[current_block] = len(graph_transactions)
    addresses[current_block] = len(np.unique(graph_addresses))
    current_block += 200

pickle.dump(transactions,open("transactions.p","wb"))
pickle.dump(addresses,open("addresses.p","wb"))
pickle.dump(transactions_blocks,open("transactions_blocks.p","wb"))
pickle.dump(addresses_blocks,open("addresses_blocks.p","wb"))
pickle.dump(times,open("times.p","wb"))

transactions = pickle.load(open("transactions.p","r"))
addresses = pickle.load(open("addresses.p","r"))
transactions_blocks = pickle.load(open("transactions_blocks.p","r"))
addresses_blocks = pickle.load(open("addresses_blocks.p","r"))
times = pickle.load(open("times.p","r"))


# PARA TAGGEDS:
stats = {}
current_block = 481935
while current_block <= 482135:
    print current_block
    graph = raw_load_graph('tagged{}-{}.p'.format(current_block, current_block+200))
    print "finding exchanges"
    exchanges =  graph.vs.select(has_tag('exchange'))
    print "finding gambling"
    gambling =  graph.vs.select(has_tag('gambling'))
    print "finding other"
    others =  graph.vs.select(has_tag('others'))
    print "finding pools"
    pools =  graph.vs.select(has_tag('pools'))
    print "finding speculators"
    speculators =  graph.vs.select(has_tag('speculator'))
    print "finding untagged"
    untagged = graph.vs.select(lambda v: (v['type'] == "Wallet" or v['type'] == "Address")
                                    and ('tag' not in v.attributes() or v['tag'] == None or v['tag'] == []))['name']
    print "finding total_volume"
    total_volume = np.sum([out['nValue'] for out in graph.es.select(type = 'OUTPUT')])
    print "finding untagged out"
    untagged_out_volume = 0
    untagged_in_volume = 0
    i = 0
    for v in untagged:
        if i % 50000 == 0:
            print "{}/{}".format(i,len(untagged))
        i+=1
        untagged_out_volume += np.sum(graph.es[graph.incident(v, mode = "out")]['nValue'])
        untagged_in_volume += np.sum(graph.es[graph.incident(v, mode = "in")]['nValue'])


    stats[current_block] = {
        'address': len(graph.vs.select(type = "Address")),
        'wallets': len(graph.vs.select(type = "Wallet")),
        'transactions': len(graph.vs.select(type = "Transaction")),
        'exchanges': len(exchanges),
        'gambling': len(gambling),
        'others': len(others),
        'pools': len(pools),
        'speculators': len(speculators),
        'untagged': {
            'addr' : len(untagged),
            'out': untagged_out_volume,
            'in': untagged_in_volume,
        },
        'total_volume': total_volume
    }
    pickle.dump(stats,open("stats.p","wb"))
    current_block += 200
print "Finished"
pickle.dump(stats,open("stats2.p","wb"))
stats = pickle.load(open("stats2.p","r"))
