import pickle
from igraph import *
from multiprocess import Process, Manager
import time
import cProfile
import json

def load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return gl

def chunkify(lst,n):
    return [lst[i::n] for i in xrange(n)]

def get_tx_links(tx, graph):
    inputs, outputs = [], []
    links = g.es.select(lambda link: link['target'] == tx['name'] or link['source'] == tx['name'])
    for link in links:
        if link['target'] == tx['name']:
            inputs += [[link['source'], link['nValue']]]
        else:
            outputs += [[link['target'], link['nValue']]]
    return {
        'inputs': inputs,
        'outputs': outputs
    }

def flow_by_group(graph, *groups):
    totales = {}
    transactions = graph.vs.select(type = "Transaction")
    for transaction in transactions:
        tx_links = get_tx_links(transaction, graph)
        inputs = set([addr for addr, _ in tx_links['inputs']])
        in_group = 'ungrouped'
        for name, vertices in groups:
            if inputs <= vertices:
                in_group = name
                break
            elif inputs & vertices != set():
                in_group = 'mixed'
                break
        outputs = set([addr for addr, _ in tx_links['outputs']])
        out_group = 'ungrouped'
        for name, vertices in groups:
            if outputs <= vertices:
                out_group = name
                break
            elif outputs & vertices != set():
                out_group = 'mixed'
                break
        totales[(in_group,out_group)] = sum(nValue for _, nValue in tx_links['inputs'])

def has_tag(tag):
    return lambda v: 'tag' in v.attributes() and v['tag'] == [tag]

def analize_block(g):
        total_volume = np.sum([out['nValue'] for out in g.es.select(type = 'OUTPUT')])
        wallets = g.vs.select(type = "Wallet")
        exchanges = g.vs.select(has_tag('exchange'))
        gambling = g.vs.select(has_tag('gambling'))
        others = g.vs.select(has_tag('others'))
        pools = g.vs.select(has_tag('pools'))



        transactions_with_exchange = g.vs[np.unique(sum([g.neighbors(exchange) for exchange in exchanges],[])).tolist()]

        job = [Process(target = find_speculators, args = (g, transactions_chunk, speculators)) for transactions_chunk  in chunkify(transactions_with_exchange,4)]
        _ = [p.start() for p in job]
        _ = [p.join() for p in job]

        def find_speculators(g, transactions_with_exchange, speculators):
            addr_who_exchanged = g.vs[np.unique(sum([g.neighbors(tx_with_exchange) for tx_with_exchange in transactions_with_exchange],[])).tolist()]
            i = 0
            for addr in addr_who_exchanged:
                i += 1
                print "{}/{}".format(i,len(addr_who_exchanged))
                addr_transactions = g.vs[g.neighbors(addr)]
                addr_communications = g.vs[np.unique(sum([g.neighbors(addr_transaction) for addr_transaction in addr_transactions],[])).tolist()]
                if not any(filter(lambda addr: not has_tag('exchange')(addr), addr_communications)):
                    #Solo intercambio con exchanges
                    speculators.append(addr)

        detected_wallets = graph.vs.select(lambda v: v['type'] == 'Wallet' and  v['tag'] is None)
        wallets_transactions = g.vs[np.unique(sum([g.neighbors(addr) for addr in wallets],[])).tolist()]

with open('exchanges.json') as exchanges_file:
    exchanges = set(x['addr'] for x in json.load(exchanges_file))
with open('gambling.json') as gambling_file:
    gambling = set(x['addr'] for x in json.load(gambling_file))
with open('others.json') as others_file:
    others = set(x['addr'] for x in json.load(others_file))
with open('pools.json') as pools_file:
    pools = set(x['addr'] for x in json.load(pools_file))


current_block = 481935

current_block = 481935
while current_block <= 487135:
    graph = load_graph('tagged{}-{}.p'.format(current_block, current_block+200))
