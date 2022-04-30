from flask import Flask, make_response, request, send_file

from pycelonis import get_celonis
from pycelonis.notebooks import api_tutorial
from pycelonis.celonis_api.pql import pql
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
import pm4py
import networkx as nx
from pm4py.algo.filtering.pandas.attributes import attributes_filter
import json
import numpy
import pandas as pd

app = Flask(__name__)

celonis = None
df = None
log = None
chartsdf = None

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route('/get_clusters/', methods=['POST', 'GET'])
def getClusters():
    print(request.form.keys())
    if 'epsilon' not in request.form.keys():
        return make_response({'error': 'No epsilon specified'}, 401)
    epsilon = request.form['epsilon']

    if 'min_pts' not in request.form.keys():
        return make_response({'error': 'No epsilon specified'}, 401)
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

@app.route('/get_charts_table/', methods=['POST', 'GET'])
def getChartsTable():
    print(request.form.keys())
    if 'epsilon' not in request.form.keys():
        return make_response({'error': 'No epsilon specified'}, 401)
    epsilon = request.form['epsilon']

    if 'min_pts' not in request.form.keys():
        return make_response({'error': 'No epsilon specified'}, 401)
    min_pts = request.form['min_pts']

    global celonis

    # GET DATA FROM CELONIS
    mobis = celonis.datamodels.find("955669d9-c78c-49eb-9982-85af2a7d1e24")
    print(mobis.tables.find('63cb0f9c-cbfd-4da0-b7a6-7232cba90992').columns)

    q = pql.PQL()
    q += pql.PQLColumn('VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) ',"VARIANTS")
    q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."ACTIVITY"',"ACTIVITY")
    q += pql.PQLColumn('MAX ( INDEX_ACTIVITY_TYPE ( "mobis_challenge_log_2019_csv"."ACTIVITY" ))',"COUNT")
    q += pql.PQLColumn('CLUSTER_VARIANTS ( VARIANT("mobis_challenge_log_2019_csv"."ACTIVITY"),' + min_pts + ', ' + epsilon +')', 'CLUSTER')

    global chartsdf
    chartsdf = mobis._get_data_frame(q)

    print(chartsdf)

    return make_response({'logs': chartsdf.to_json()})

@app.route('/get_dfg/', methods=['POST', 'GET'])
def getDFG():
    # log = pm4py.convert_to_event_log(df)
    global log
    print("Log NNone: " + str(log is None))
    dfg = dfg_discovery.apply(log)

    ngraph = nx.DiGraph()
    ngraph.add_edges_from(dfg)
    node_dict = {"node" + str(i+1): {"name" : list(ngraph.nodes)[i]} for i in range(len(ngraph.nodes))}
    edge_dict = {"edge" + str(i+1): {"source" : "node" + str(list(ngraph.nodes).index(list(ngraph.edges)[i][0]) + 1) ,"target": "node" + str(list(ngraph.nodes).index(list(ngraph.edges)[i][1]) + 1)}  for i in range(len(ngraph.edges))}
    return make_response({'dfg_nodes': node_dict,
                          'dfg_edges': edge_dict})

@app.route('/get_filter', methods=['POST', 'GET'])
def filterAct():
    global log
    log_df = pm4py.convert_to_dataframe(log)
    filter_out_acts = None

    print(request.form.keys())
    print('list_act' in request.form.keys())

    if 'list_act' in request.form.keys():
        filter_out_acts = request.form['list_act']
    else:
        return make_response({'error': " 'list_act' is missing"}, 401)

    # try:
    filter_out_acts = '{\"list\":' + filter_out_acts + '}'
    print(filter_out_acts)
    filter_out_acts = json.loads(filter_out_acts)
    filter_out_acts = filter_out_acts['list']
    print(filter_out_acts)

    print(df.loc[0]['concept:name'] == filter_out_acts[0])

    print(len(df.index))
    for x in df.index:
        if df.loc[x]['concept:name'] in filter_out_acts:
            df.drop(x, inplace = True)
    print(len(df.index))
    return make_response({'logs': df.to_json()})

@app.route('/get_filter_cluster/', methods=['POST','GET'])
def filterCluster():
    global log
    log_df = pm4py.convert_to_dataframe(log)
    filter_out_clusters = None

    print(request.form.keys())
    print('list_act' in request.form.keys())

    if 'list_act' in request.form.keys():
        filter_out_clusters = request.form['list_act']
    else:
        return make_response({'error': " 'list_act' is missing"}, 401)

    # try:
    filter_out_clusters = '{\"list\":' + filter_out_clusters + '}'
    print(filter_out_clusters)
    filter_out_clusters = json.loads(filter_out_clusters)
    filter_out_clusters = filter_out_clusters['list']
    print(filter_out_clusters)

    # print(df.loc[0]['Cluster'] == filter_out_clusters[0])

    print(len(df.index))
    for x in df.index:
        if df.loc[x]['Cluster'] in filter_out_clusters:
            df.drop(x, inplace=True)
    print(len(df.index))
    return make_response({'logs': df.to_json()})

@app.route('/get_activity_freq/', methods=['POST', 'GET'])
def getFreq():
    pass
    # return make_response({'result': dfg})

@app.route('/get_cluster_chart/', methods=['POST','GET'])
def getClusterChart():
    if 'cluster' not in request.form.keys():
        return make_response({'error': 'No cluster specified'}, 401)
    cluster = request.form['cluster']

    global chartsdf
    sum_array = []
    activities = chartsdf['ACTIVITY'].unique()

    print(chartsdf)
    for activity in activities:
        sum_array.append(chartsdf.loc[(chartsdf["CLUSTER"] == int(cluster)) & (chartsdf["ACTIVITY"] == activity)]["COUNT"].sum())
    max_sum = max(sum_array)
    return_array = [item / max_sum  for item in sum_array]
    return make_response({'chart_data': return_array})
