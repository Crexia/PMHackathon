from flask import Flask, make_response, request, send_file

from pycelonis import get_celonis
from pycelonis.notebooks import api_tutorial
from pycelonis.celonis_api.pql import pql
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import pm4py

app = Flask(__name__)

celonis = None
df = None

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
    df = mobis._get_data_frame(q)

    log = pm4py.convert_to_event_log(df)

    # xes_exporter.apply(log, 'logs.xes')

    # dfg = dfg_discovery.apply(log)
    # df.head()

    # return send_file('logs.xes', attachment_filename='logs.xes')
    return make_response({'logs': df.to_json()})

@app.route('/get_dfg/', methods=['POST', 'GET'])
def getDFG():
    dfg = dfg_discovery.apply(log)
    print(dfg)
    return make_response({'dfg': dfg})
