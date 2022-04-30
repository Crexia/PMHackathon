from cProfile import label
from pycelonis import get_celonis,pql
import pm4py
import pandas as pd
import matplotlib.pyplot as plt



celonis = get_celonis(
          url= "https://academic-umut-dural-rwth-aachen-de.eu-2.celonis.cloud/",
          api_token= "MjIyYjViMDEtMDE4Yi00YTM0LWI0OGYtZjRlNjgyNDBkZmYyOnQySkRBV0hyUmFjUllWUldBeXZHaFFzZHRPN1psL21SVWFScXh0UjdUUkRE"
)

mobis = celonis.datamodels.find("955669d9-c78c-49eb-9982-85af2a7d1e24")

q = pql.PQL()
q += pql.PQLColumn('VARIANT ( "mobis_challenge_log_2019_csv"."ACTIVITY" ) ',"VARIANTS")
q += pql.PQLColumn('"mobis_challenge_log_2019_csv"."ACTIVITY"',"ACTIVITY")
q += pql.PQLColumn('MAX ( INDEX_ACTIVITY_TYPE ( "mobis_challenge_log_2019_csv"."ACTIVITY" ))',"COUNT")
q += pql.PQLColumn(' CLUSTER_VARIANTS(VARIANT("mobis_challenge_log_2019_csv"."ACTIVITY") , 87 , 3 )',"CLUSTER")
# q += pql.PQLFilter(' Filter CLUSTER_VARIANTS(VARIANT("mobis_challenge_log_2019_csv"."ACTIVITY") , 87 , 3 )  = 4')

variant_data = mobis.get_data_frame(q)


def get_variant_table(variant):
    return_table = variant_data.loc[variant_data['VARIANTS'] == variant]
    return return_table

activities = variant_data['ACTIVITY'].unique()

variant_table = get_variant_table(variant_data['VARIANTS'].unique()[70])



def plot_variant(variant):
    count_array = [variant.loc[variant["ACTIVITY"] == activity]["COUNT"].to_list()[0] if activity in variant["ACTIVITY"].unique() else 0 for activity in activities]
    plot_array = [item / max(count_array) for item in count_array]
    plt.bar(range(len(activities)), plot_array)
    plt.show()


def plot_cluster(cluster):
    sum_array = []
    for activity in activities:
        sum_array.append(variant_data.loc[(variant_data["CLUSTER"] == cluster) & (variant_data["ACTIVITY"] == activity)]["COUNT"].sum())
    max_sum = max(sum_array)
    plot_array = [item / max_sum  for item in sum_array]
    plt.bar(range(len(activities)), plot_array)

plot_variant(variant_table)

plt.show()
