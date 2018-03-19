import pickle
from igraph import *
from multiprocess import Process, Manager
import time
import cProfile
import json
import numpy as np

def load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return gl

def chunkify(lst,n):
    return [lst[i::n] for i in xrange(n)]

def do_tag(vertex, tag):
    if 'tag' not in vertex.attributes() or vertex['tag'] is None:
        vertex['tag'] = [tag]
    else:
        vertex['tag'] += [tag]

def has_tag(tag):
    return lambda v: 'tag' in v.attributes() and v['tag'] is not None and (v['tag'] == [tag] or tag in v['tag'])

with open('exchanges.json') as exchanges_file:
    exchanges = set(x['addr'] for x in json.load(exchanges_file))
with open('gambling.json') as gambling_file:
    gambling = set(x['addr'] for x in json.load(gambling_file))
with open('others.json') as others_file:
    others = set(x['addr'] for x in json.load(others_file))
with open('pools.json') as pools_file:
    pools = set(x['addr'] for x in json.load(pools_file))

manager = Manager()
speculators = manager.dict()

current_block = 481935
while current_block <= 483535:
    graph = load_graph('recognized{}-{}.p'.format(current_block, current_block+200))

    def tag_vertices(vertices, tags, current_block):
        def tag(vertex, vtags, tags):
            if vertex['name'] not in tags:
                tags[vertex['name']] = []
            for vtag in vtags:
                tags[vertex['name']] += [vtag]
        i = 0
        tagged_addr = 0
        tagged_wallet = 0
        for vertex in vertices:
            if i % (len(vertices) / 5) == 0:
                print "{}/{} tagged_addr{} tagged_wallet{} cblock{}".format(i, len(vertices),tagged_addr, tagged_wallet, current_block)
            i+=1
            if vertex['type'] == "Address":
                if vertex['name'] in exchanges:
                    tag(vertex,['exchange'],tags)
                    tagged_addr +=1
                elif vertex['name'] in gambling:
                    tag(vertex,['gambling'],tags)
                    tagged_addr +=1
                elif vertex['name'] in others:
                    tag(vertex,['others'],tags)
                    tagged_addr +=1
                elif vertex['name'] in pools:
                    tag(vertex,['pools'],tags)
                    tagged_addr +=1
            elif vertex['type'] == "Wallet":
                if any(addr for addr in vertex['addresses'] if addr in exchanges):
                    tag(vertex,['exchange'],tags)
                    tagged_wallet +=1
                elif any(addr for addr in vertex['addresses'] if addr in gambling):
                    tag(vertex,['gambhttp://localhost:8888/tree?token=655075cac2189e9c5e248b438885d3cc5b4af15466910ff5ling'],tags)
                    tagged_wallet +=1
                elif any(addr for addr in vertex['addresses'] if addr in others):
                    tag(vertex,['others'],tags)
                    tagged_wallet +=1
                elif any(addr for addr in vertex['addresses'] if addr in pools):
                    tag(vertex,['pools'],tags)
                    tagged_wallet +=1

    tags = manager.dict()
    vertices = graph.vs.select(type_in = ["Wallet","Address"])
    job = [Process(target = tag_vertices, args = (vertices_chunk, tags, current_block)) for vertices_chunk  in chunkify(vertices,4)]
    _ = [p.start() for p in job]
    _ = [p.join() for p in job]
    print "Vertex tagging finished, now applying"
    for vertex in vertices:
        if vertex['name'] in tags:
            for vtag in tags[vertex['name']]:
                do_tag(vertex, vtag)
    print "Applying finished"
    pickle.dump(graph,open("tagged{}-{}.p".format(current_block, current_block+200),"wb"))
    #
    # print "Proceed to find speculators in {}".format(current_block)
    # exchanges_v = graph.vs.select(has_tag('exchange'))
    # transactions_with_exchange = graph.vs[np.unique(sum([graph.neighbors(exchange) for exchange in exchanges_v],[])).tolist()]
    #
    # def find_speculators(g, transactions_with_exchange, speculators):
    #     addr_who_exchanged = g.vs[np.unique(sum([g.neighbors(tx_with_exchange) for tx_with_exchange in transactions_with_exchange],[])).tolist()]
    #     i = 0
    #     for addr in addr_who_exchanged:
    #         i += 1
    #         if i % 10000 == 0:
    #             print "{}/{}".format(i,len(addr_who_exchanged))
    #         addr_transactions = g.vs[g.neighbors(addr)]
    #         addr_communications = g.vs[np.unique(sum([g.neighbors(addr_transaction) for addr_transaction in addr_transactions],[])).tolist()]
    #         if not any(filter(lambda other_addr: not has_tag('exchange')(other_addr) and other_addr['name'] != addr['name'], addr_communications)):
    #             #Solo intercambio con exchanges
    #             speculators[addr['name']] = 'algo'
    #         else:
    #             if addr['name'] in speculators:
    #                 del speculators[addr['name']]
    #
    # job = [Process(target = find_speculators, args = (graph, transactions_chunk, speculators)) for transactions_chunk  in chunkify(transactions_with_exchange,4)]
    # _ = [p.start() for p in job]
    # _ = [p.join() for p in job]
    # print "Speculators by now: {}".format(len(speculators))
    current_block += 200

# #484735
# print "Finished finding, now tagging speculators"
# current_block = 481935
# while current_block <= 483535:
#     graph = load_graph('tagged{}-{}.p'.format(current_block, current_block+200))
#     vertices = graph.vs.select(type_in = ["Wallet","Address"])
#     i = 0
#     for v in vertices:
#         if i % (len(vertices) / 5) == 0:
#             print "tagging speculators {}/{} current_block {}".format(i,len(vertices),current_block)
#         i+= 1
#         if v['name'] in speculators:
#             do_tag(v,'speculator')
#
#     print "{} tagged specualtros  in {}".format(len(graph.vs.select(has_tag('speculator'))),current_block)
#     pickle.dump(graph,open("tagged{}-{}.p".format(current_block, current_block+200),"wb"))
#     current_block += 200
