import pickle
from igraph import *
from multiprocess import Process, Manager
import time
import cProfile

def load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return gl
    #return Graph.DictList(gl['vertices'].values(),gl['edges'],directed=True)

def chunkify(lst,n):
    return [lst[i::n] for i in xrange(n)]

def find_known_wallets(wallets, list_global_wallets, global_wallets, found_equivalences, current_block):
    i = 0
    findings = 0
    for w in wallets:
        if i % 8000 == 0:
            print "{}/{} findings{} globalwallet c {} {}".format(i,len(wallets),findings,len(global_wallets),current_block)
        i += 1
        found = False
        for wname, wset in list_global_wallets:
            # Conforma la misma wallet, uno
            if w['addresses'].intersection(wset) != set():
                 found_equivalences[w['name']] = [wname,w['addresses'].union(wset)]
                 findings += 1
                 found = True
                 break
        if not found:
            global_wallets[w['name']] = w['addresses']
    print "{}/{} findings{} globalwallet c {}".format(i,len(wallets),findings,len(global_wallets),current_block)



current_block = 481935
manager = Manager()
global_wallets = manager.dict()
#global_wallets = {} 483535
while current_block <= 483535:
    graph = load_graph('entidades{}-{}.p'.format(current_block, current_block+200))
    wallets = graph.vs.select(type = "Wallet")
    print "wallets length es {}".format(len(wallets))
    wallet_index = {}
    for w in wallets:
        wallet_index[w['name']] = w
    found_equivalences = manager.dict()
    #find_known_wallets(wallets, global_wallets, current_block)
    job = [Process(target = find_known_wallets, args = (wallets_chunk, global_wallets.items(), global_wallets, found_equivalences, current_block)) for wallets_chunk  in chunkify(wallets,4)]
    _ = [p.start() for p in job]
    _ = [p.join() for p in job]

    print "found equivalences {}".format(len(found_equivalences.keys()))
    i = 0
    for w in found_equivalences.keys():
        if i % 1000 == 0:
            print "{}/{}".format(i,len(found_equivalences.keys()))
        i += 1
        node = wallet_index[w]
        #node = graph.vs.find(name = w)
        new_name, new_set = found_equivalences[w]
        for v in graph.es.select(source = node['name']):
            v['source'] = new_name
        for v in graph.es.select(target = node['name']):
            v['target'] = new_name
        node['name'] = new_name
        node['addresses'] = new_set
    print "finished block, global wallets len is {}".format(len(global_wallets))
    pickle.dump(graph,open("recognized{}-{}.p".format(current_block, current_block+200),"wb"))
    current_block += 200
