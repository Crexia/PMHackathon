from pycelonis import get_celonis,pql
import pm4py
from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
from pm4py.visualization.dfg import visualizer as dfg_visualization

import networkx as nx

celonis = get_celonis(
          url= "https://academic-umut-dural-rwth-aachen-de.eu-2.celonis.cloud/",
          api_token= "MjIyYjViMDEtMDE4Yi00YTM0LWI0OGYtZjRlNjgyNDBkZmYyOnQySkRBV0hyUmFjUllWUldBeXZHaFFzZHRPN1psL21SVWFScXh0UjdUUkRE"
)

mobis = celonis.datamodels.find("955669d9-c78c-49eb-9982-85af2a7d1e24")

q = pql.PQL()
q += pql.PQLColumn('"mobis_challenge_log_2019_csv_CASES"."CASE"',"case:concept:name")
q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."ACTIVITY"',"concept:name")
q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."START"',"time:timestamp")
# q += pql.PQLColumn(' CLUSTER_VARIANTS(VARIANT("mobis_challenge_log_2019_csv"."ACTIVITY") , 87 , 3 )',"Clusters")
q += pql.PQLFilter(' Filter CLUSTER_VARIANTS(VARIANT("mobis_challenge_log_2019_csv"."ACTIVITY") , 87 , 3 )  = 4')
mobis_table = mobis.get_data_frame(q)



log = pm4py.convert_to_event_log(mobis_table)

dfg = dfg_discovery.apply(log)

gviz = dfg_visualization.apply(dfg, log=log, variant=dfg_visualization.Variants.FREQUENCY)
dfg_visualization.view(gviz)


G = nx.DiGraph()
G.add_edges_from(dfg)

node_dict = {"node" + str(i+1): {"name" : list(G.nodes)[i]} for i in range(len(G.nodes))}
edge_dict = {"edge" + str(i+1): {"source" : "node" + str(list(G.nodes).index(list(G.edges)[i][0]) + 1) ,"target": "node" + str(list(G.nodes).index(list(G.edges)[i][1]) + 1)}  for i in range(len(G.edges))}
print(edge_dict)

# v_q = pql.PQL()
# v_q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."ACTIVITY"')
# v_q += pql.PQLColumn('')







