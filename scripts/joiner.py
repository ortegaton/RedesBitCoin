import pandas as pd
import pickle
import igraph
import glob

# # RAW:
# current_block = 481935
# while current_block <= 491535:
#     str_name = '{}-{}.p'.format(current_block, current_block+200)
#     gl =  pickle.load(open(str_name, "rb"))
#
#     df_edges = pd.DataFrame(gl['edges'])
#     df_edges.columns = ['nValue',':START_ID',':END_ID',':TYPE']
#     df_vertices = pd.DataFrame(gl['vertices'].values())
#
#     df_address = df_vertices[df_vertices['type'] == "Address"]
#     df_address.drop('block',1, inplace= True)
#     df_address.drop('time',1, inplace= True)
#     df_address.drop('timestamp',1, inplace= True)
#     df_address.columns = ['name:ID',':LABEL']
#
#     df_transaction =  df_vertices[df_vertices['type'] == "Transaction"]
#     df_transaction.columns = ['block','name:ID','time','timestamp',':LABEL']
#
#     df_edges.to_csv('csv/{}edges.csv'.format(current_block))
#     df_address.to_csv('csv/{}address.csv'.format(current_block))
#     df_transaction.to_csv('csv/{}transaction.csv'.format(current_block))

def load_graph(str_name):
    gl =  pickle.load(open(str_name, "rb"))
    return gl

#TAGGED: 483535
current_block = 481935
cols = {}
while current_block <= 482135:
    print current_block
    str_name = 'tagged{}-{}.p'.format(current_block, current_block+200)
    graph = load_graph('tagged{}-{}.p'.format(current_block, current_block+200))
    # for v in graph.vs.select(type = "Wallet"):
    #     v['name'] = v['name'].split('_')[0]
    # pickle.dump(graph,open("tagged{}-{}.p".format(current_block, current_block+200),"wb"))

    gl = {
        'vertices':{},
        'edges': []
    }

    splitags = lambda s: [x[1:-1] for x in str(s)[1:-1].split()]
    is_exchange = lambda x: len([t for t in splitags(x) if 'exchange' in t]) > 0
    is_other = lambda x: len([t for t in splitags(x) if 'other' in t]) > 0
    is_gambling = lambda x: len([t for t in splitags(x) if 'gamb' in t]) > 0
    is_pool = lambda x: len([t for t in splitags(x) if 'pool' in t]) > 0
    for v in graph.vs:
        timestamp = None

        block = None
        if v['timestamp'] is not None:
            timestamp = int(v['timestamp'])
        if v['block'] is not None:
            block = int(v['block'])
        gl['vertices'][v['name']] = {
            "name": v['name'],
            "type": v['type'],
            "timestamp": timestamp,
            "time": v['time'],
            "block": block,
            "tags": v['tag'],
            "exchange": str(is_exchange(v['tag'])).lower(),
            "other": str(is_other(v['tag'])).lower(),
            "gambling": str(is_gambling(v['tag'])).lower(),
            "pool": str(is_pool(v['tag'])).lower(),
            "addresses": v['addresses']
        };
    for e in graph.es:
        gl['edges'].append({
            "source": e['source'],
            "target" : e['target'],
            "type" : e['type'],
            "nValue" : e['nValue']
        });

    df_edges = pd.DataFrame(gl['edges'])
    df_edges.columns = ['nValue:float',':START_ID',':END_ID',':TYPE']
    df_vertices = pd.DataFrame(gl['vertices'].values())

    df_address = df_vertices[df_vertices['type'] == "Address"]
    df_address.drop('block',1, inplace= True)
    df_address.drop('time',1, inplace= True)
    df_address.drop('timestamp',1, inplace= True)
    df_address.drop('addresses', 1, inplace= True)
    df_address.columns = ['exchange','gambling','name:ID','other','pool','tags',':LABEL']

    df_wallets = df_vertices[df_vertices['type'] == "Wallet"]
    df_wallets.drop('block',1, inplace= True)
    df_wallets.drop('time',1, inplace= True)
    df_wallets.drop('timestamp',1, inplace= True)
    df_wallets.columns = ['addresses','exchange','gambling','name:ID','other','pool','tags',':LABEL']

    df_transaction =  df_vertices[df_vertices['type'] == "Transaction"]
    df_transaction.drop('addresses', 1, inplace= True)
    df_transaction.drop('tags', 1, inplace= True)
    df_transaction.drop('exchange', 1, inplace= True)
    df_transaction.drop('other', 1, inplace= True)
    df_transaction.drop('gambling', 1, inplace= True)
    df_transaction.drop('pool', 1, inplace= True)
    df_transaction[['block']] = df_transaction[['block']].astype(int)
    df_transaction[['timestamp']] = df_transaction[['timestamp']].astype(int)
    df_transaction.columns = ['block:int','name:ID','time','timestamp:int',':LABEL']

    df_edges.to_csv('csv/{}edges.csv'.format(current_block), index = False)
    df_address.to_csv('csv/{}address.csv'.format(current_block), index = False)
    df_wallets.to_csv('csv/{}wallets.csv'.format(current_block), index = False)
    df_transaction.to_csv('csv/{}transaction.csv'.format(current_block))
    cols['address'] = df_address.columns
    cols['transaction'] = df_transaction.columns
    cols['wallets'] = df_wallets.columns
    cols['edges'] = df_edges.columns

    current_block += 200

# REALLY JOIN THEM ALL
types = ['edges', 'transaction', 'address', 'wallets']
for type_f in types:
    allFiles = glob.glob("csv/*{}.csv".format(type_f))
    print type_f
    print len(allFiles)
    frame = pd.DataFrame()
    list_ = []
    for file_ in allFiles:
        df = pd.read_csv(file_, usecols=cols[type_f])
        list_.append(df)
    frame = pd.concat(list_)
    if type_f == 'transaction':
        frame[['block:int']] = frame[['block:int']].astype(int)
        frame[['timestamp:int']] = frame[['timestamp:int']].astype(int)
    frame.to_csv('csv/all{}.csv'.format(type_f), index = False)
