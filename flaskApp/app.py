from flask import Flask, make_response, request, send_file

from pycelonis import get_celonis
from pycelonis.notebooks import api_tutorial
from pycelonis.celonis_api.pql import pql
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py
import networkx as nx

app = Flask(__name__)

celonis = None
df = None
log = None

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route('/get_clusters/', methods=['POST', 'GET'])
def getClusters():
    print(request.form.keys())
    if 'epsilon' not in request.form.keys():
        return make_response({'error': 'No epsilon specified'})
    epsilon = request.form['epsilon']

    if 'min_pts' not in request.form.keys():
        return make_response({'error': 'No epsilon specified'})
    min_pts = request.form['min_pts']

    global celonis
    celonis = get_celonis(
        url="https://academic-umut-dural-rwth-aachen-de.eu-2.celonis.cloud/",
        api_token="MjIyYjViMDEtMDE4Yi00YTM0LWI0OGYtZjRlNjgyNDBkZmYyOnQySkRBV0hyUmFjUllWUldBeXZHaFFzZHRPN1psL21SVWFScXh0UjdUUkRE"
    )

    # GET DATA FROM CELONIS
    mobis = celonis.datamodels.find("955669d9-c78c-49eb-9982-85af2a7d1e24")
    print(mobis.tables.find('63cb0f9c-cbfd-4da0-b7a6-7232cba90992').columns)

    q = pql.PQL()
    q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."START"', 'time:timestamp')
    q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."CASE"', 'case:concept:name')
    q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."ACTIVITY"', "concept:name")
    q += pql.PQLColumn('CLUSTER_VARIANTS ( VARIANT("mobis_challenge_log_2019_csv"."ACTIVITY"),' + min_pts + ', ' + epsilon +')', 'Cluster')
    global df
    df = mobis._get_data_frame(q)

    global log
    log = pm4py.convert_to_event_log(df)

    # xes_exporter.apply(log, 'logs.xes')

    # dfg = dfg_discovery.apply(log)
    # df.head()

    # return send_file('logs.xes', attachment_filename='logs.xes')
    return make_response({'logs': df.to_json()})

@app.route('/get_dfg/', methods=['POST', 'GET'])
def getDFG():
    # log = pm4py.convert_to_event_log(df)
    global log
    print("Log NNone: " + str(log is None))
    dfg = dfg_discovery.apply(log)

    ngraph = nx.DiGraph()
    ngraph.add_edges_from(dfg)
    # print("Is gra")
    json_ngraph = nx.node_link_data(ngraph)
    # print(json_ngraph)
    print(ngraph.nodes)
    print(ngraph.edges)
    node_dict = {"node" + str(i+1): {"name" : list(ngraph.nodes)[i]} for i in range(len(ngraph.nodes))}
    edge_dict = {"edge" + str(i+1): {"source" : "node" + list(ngraph.nodes).index(list(ngraph.edges)[i][0]) + 1 ,"target": "node" + list(ngraph.nodes).index(list(ngraph.edges)[i][1]) + 1}  for i in range(len(ngraph.edges))}
    return make_response({'dfg_nodes': node_dict,
                          'dfg_edges': edge_dict})

@app.route('/get_activity_freq/', methods=['POST', 'GET'])
def getFreq():
    pass
    # return make_response({'result': dfg})
