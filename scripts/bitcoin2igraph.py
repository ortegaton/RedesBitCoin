import bitcoin.rpc
from bitcoin.core import b2lx
from datetime import datetime
import time
from blockchain import blockexplorer
from bitcoin.wallet import CBitcoinAddress
from igraph import *
import pickle

def to_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%d-%m-%Y')

def find_bolzano(timestamp):
    bitcoind = bitcoin.rpc.Proxy()
    best_blockhash = bitcoind.getbestblockhash()
    best_blockheader =  bitcoind.getblockheader(best_blockhash, verbose = True)
    best_height = best_blockheader['height']
    init_height = 1
    end_height = best_height
    found = False
    while not found:
        search_height = (init_height + end_height) / 2
        search_blockhash = bitcoind.getblockhash(search_height)
        search_blockheader = bitcoind.getblockheader(search_blockhash, verbose = True)
        search_time = search_blockheader['mediantime']
        if search_time > timestamp:
            end_height = search_height
        elif search_time < timestamp:
            init_height = search_height
        if end_height == init_height + 1:
            found = True
    return (init_height,end_height)

def unique_vertex(graph_list, name, node_type, timestamp = None, time = None, block = None):
    if name not in graph_list['vertices']:
        graph_list['vertices'][name] = {
            "name": name,
            "type": node_type,
            "timestamp": timestamp,
            "time": time,
            "block": block
        };

def unique_edge(graph_list, source, target, node_type, nValue):
    graph_list['edges'].append({
        "source": source,
        "target" : target,
        "type" : node_type,
        "nValue" : nValue
    });


str_from = '05-05-2016'
str_to = '8-05-2016'


def get_graph_list(block_from, block_to):
    graph_list = {
        'edges': [],
        'vertices': {}
    }
    for height in range(block_from,block_to+1):
        try :
            bitcoind = bitcoin.rpc.Proxy()
            progress = (height - block_from) * 100 / (block_to - block_from)
            print "{} Processing block #{}/{}".format(progress,height,block_to+1)

            block_hash = bitcoind.getblockhash(height)
            block = bitcoind.getblock(block_hash)
            iter_tx = 0
            total_txs = len(block.vtx)
            for tx in block.vtx:
                try:
                    if not tx.is_coinbase():
                        iter_tx += 1
                        transaction_info = bitcoind.getrawtransaction(tx.GetTxid(), verbose = True)
                        str_time = datetime.utcfromtimestamp(transaction_info['time']).strftime('%Y-%m-%d %H:%M:%S')
                        txid = b2lx(tx.GetTxid())
                        unique_vertex(graph_list, txid, node_type = "Transaction", timestamp = transaction_info['time'], time = str_time, block = height)
                        #print "{} Block #{}/{}  - Processing transaction #{} - {}/{}".format(progress,height,to_height+1, b2lx(tx.GetTxid()),iter_tx, total_txs)

                        for txin in tx.vin:
                            tx_origin = bitcoind.getrawtransaction(txin.prevout.hash)
                            nValue = tx_origin.vout[txin.prevout.n].nValue
                            address = CBitcoinAddress.from_scriptPubKey(tx_origin.vout[txin.prevout.n].scriptPubKey).__str__()
                            unique_vertex(graph_list, address, node_type = "Address")
                            unique_edge(graph_list, address, txid, node_type = "INPUT", nValue = nValue * 0.00000001)

                        for txout in tx.vout:
                            nValue = txout.nValue
                            address = CBitcoinAddress.from_scriptPubKey(txout.scriptPubKey).__str__()
                            unique_vertex(graph_list, address, node_type = "Address")
                            unique_edge(graph_list, txid, address, node_type = "OUTPUT",  nValue = nValue *  0.00000001)

                except Exception as ex:
                    pass
        except Exception as ex:
            print ex
            print "Fail, lost block"
    return graph_list

# for i in range(22,30):
#     print "============================================"
#     print "============================================"
#     str_from = '{}-05-2016'.format(i)
#     str_to = '{}-05-2016'.format(i+1)
#     print str_from
#     gl = get_graph_list(str_from, str_to)
#     pickle.dump( gl, open( "{}mayo.p".format(i), "wb" ) )

i_block = 489135
while True:
    print i_block
    next_i_block = i_block + 600
    gl = get_graph_list(i_block, next_i_block)
    pickle.dump( gl, open( "600-{}-{}.p".format(i_block,next_i_block), "wb" ) )
    i_block = next_i_block
