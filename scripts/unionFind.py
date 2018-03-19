
import pickle
from igraph import *
import equivalence
import numpy as np

reload(equivalence)


def load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return Graph.DictList(gl['vertices'].values(),gl['edges'],directed=True)


def chunkify(lst,n):
    return [lst[i::n] for i in xrange(n)]

#graph = load_graph('481935-482135.p')

def create_equivalence(graph, current_block):
    # CREO CLASES DE EQUIVALENCIA ENTRE DIRECCIONES
    equivalence_class = equivalence.BidirectionalEquivalence()
    current_tx = 0
    transactions = graph.vs.select(type = "Transaction")
    current_class = 0
    for transaction in transactions:
        input_addresses = graph.vs[graph.neighbors(transaction,mode = "IN")]['name']
        # Si solo hay una input address no aporta informacion
        if len(input_addresses) <= 1:
            current_tx += 1
            continue
        # Cuando empiezo una wallet le pongo una id a la clase de equivalencia
        if equivalence_class.partition(input_addresses[0]) == set():
            input_addresses.append('w#{}'.format(current_class))
            current_class += 1
        equivalence_class.merge(*input_addresses)
        print "Equivalencias {}/{} {}".format(current_tx,len(transactions), current_block)
        current_tx += 1
    return equivalence_class



def reconstruct_transaction(transactions, graph, equivalence, edges, vertices, current_block):
    i = 0
    for transaction in transactions:
        if i % 15000 == 0:
            print "{}/{} {}".format(i,len(transactions),current_block)
        i+=1
        try:
            # TRANSACTION VERTEX
            vertices[transaction['name']] = transaction.attributes()
            input_addresses = graph.vs[graph.neighbors(transaction,mode = "IN")]
            if input_addresses == 0:
                print "me fui"
                continue
            first_input_address = input_addresses[0]
            equivalence_class = equivalence.partition(first_input_address['name'])
            # NO HAY CLASE DE EQUIVALENCIA PARA LOS INPUT, ES UN ADDR
            if equivalence_class == set():
                assert len(input_addresses) == 1
                vertices[first_input_address['name']] = first_input_address.attributes()
                edges.append(graph.es[graph.incident(transaction,mode="IN")][0].attributes())
            # SON VARIOS INPUT, TODOS DEBERIAN ESTAR EN LA MISMA
            else:
                wallet_names = [ filter(lambda x: x.startswith('w#'), equivalence.partition(input_addr['name']))[0] for input_addr in input_addresses]
                assert len(np.unique(wallet_names)) == 1
                wallet_name =  wallet_names[0]
                vertices[wallet_name] = {
                    'name' : wallet_name,
                    'type' : 'Wallet',
                    'addresses': equivalence_class
                }
                # WALLET INPUT LINK
                edges.append({
                    'source': wallet_name,
                    'target': transaction['name'],
                    'type' : 'INPUT',
                    'nValue': sum(graph.es[graph.incident(transaction,mode="IN")]['nValue'])
                })
            # TRANSACTION OUTPUTS
            output_addresses = graph.vs[graph.neighbors(transaction,mode = "OUT")]
            out_equivalences = equivalence.partitions(output_addresses['name'])
            for out_equivalence in out_equivalences:
                equivalence_class = equivalence.partition(out_equivalence[0])
                if equivalence_class == set():
                    # OUTPUT ADDRESS
                    address_name = out_equivalence[0]
                    vertices[address_name] = output_addresses.find(name = address_name).attributes()
                    new_edge = graph.es[graph.incident(transaction,mode="OUT")].find(target = address_name).attributes()
                    new_edge['origen'] = 'single'
                    # OUTGOING LINK TO ADDRESS
                    edges.append(
                        new_edge
                    )
                else :
                    # OUTPUT WALLET
                    wallet_name =  filter(lambda x: x.startswith('w#'), equivalence_class)[0]
                    equivalence_class.remove(wallet_name)
                    vertices[wallet_name] = {
                        'name' : wallet_name,
                        'type' : 'Wallet',
                        'addresses': equivalence_class
                    }
                    # OUTGOING LINK TO WALLET
                    edges.append({
                        'source': transaction['name'],
                        'target': wallet_name,
                        'type' : 'OUTPUT',
                        'nValue': sum(graph.es[graph.incident(transaction,mode="OUT")].select(target_in = out_equivalence)['nValue']),
                        'origen': 'joined'
                    })
        except Exception as e:
            print e

from multiprocess import Process, Manager

# DE A 200
current_block = 482335
while current_block <= 483535:
    graph = load_graph('{}-{}.p'.format(current_block, current_block+200))
    equivalence_class = create_equivalence(graph, current_block)

    # RECONSTRUYO EL GRAFO
    # Excluye al ultimo bloque del grupo porque lo processa el siguiente
    transactions = graph.vs.select(type = "Transaction", block_lt = current_block+200)
    manager = Manager()

    edges = manager.list()
    vertices = manager.dict()
    job = [Process(target = reconstruct_transaction, args = (transaction_chunk, graph, equivalence_class, edges, vertices, current_block )) for transaction_chunk in chunkify(transactions,4)]
    _ = [p.start() for p in job]
    _ = [p.join() for p in job]
    entity_graph = Graph.DictList(vertices.values(),edges,directed=True)

    pickle.dump(entity_graph,open("entidades{}-{}.p".format(current_block, current_block+200),"wb"))
    current_block += 200


# # DE A 600
# current_block = 481935
# while current_block <= 489135:
#     graph = load_graph('600-{}-{}.p'.format(current_block, current_block+600))
#     equivalence_class = create_equivalence(graph)
#
#     # RECONSTRUYO EL GRAFO
#     transactions = graph.vs.select(type = "Transaction")
#     manager = Manager()
#
#     edges = manager.list()
#     vertices = manager.dict()
#     job = [Process(target = reconstruct_transaction, args = (transaction_chunk, graph, equivalence_class, edges, vertices)) for transaction_chunk in chunkify(transactions,4)]
#     _ = [p.start() for p in job]
#     _ = [p.join() for p in job]
#     entity_graph = Graph.DictList(vertices.values(),edges,directed=True)
#
#     pickle.dump(entity_graph,open("600entidades{}-{}.p".format(current_block, current_block+600),"wb"))
#     current_block += 600
