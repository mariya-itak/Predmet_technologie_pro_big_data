# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


# Press the green button in the gutter to run the script.
import pprint
from bson import Code
from pymongo import MongoClient
from pymongo.collation import Collation

client = MongoClient('localhost', 27017)
db = client.cv03
collection = db.idnes

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def print_documents(query):
	for item in query:
		print(item)


if __name__ == '__main__':
    mapper = Code("""function() {emit( this.date.substring(0, 4), 1)}""")
    reducer = Code("""function(key, values){ return Array.sum(values);}""")
    result = collection.map_reduce(mapper, reducer, "myResult")
    data = result.find({"_id": {'$ne': ''}}, {"date": 1, "value": 1}).sort("_id", 1)
    data_line = list(data)
    year = [data_line[0]["_id"]]
    count = [data_line[0]["value"]]
    for i in range(1, len(list(data_line))):
        year.append(data_line[i]["_id"])
        count.append(data_line[i]["value"])
    df = pd.DataFrame(dict(
        x=year,
        y=count
    ))
    fig = px.line(df, x="x", y="y")
    #fig.show()

    fig = px.bar(x=year, y=count)
    #fig.show()

    result = collection.find({}, {"_id": 0, "content": 1, "comments": 1}).sort("comments", -1).collation(Collation(locale='en_US', numericOrdering=True)).limit(1000)
    df = pd.DataFrame(data=result)
    for i in range(0, len(df['content'])):
        df['content'][i] = len(str(df['content'][i]).split(' '))
    fig = px.scatter(x=df['content'], y=df['comments'])
    #fig.show()

    mapper = Code("""function() {emit( this.category, 1)}""")
    reducer = Code("""function(key, values){ return Array.sum(values);}""")
    result = collection.map_reduce(mapper, reducer, "myResult")
    res_sorted = result.find({}, {"date": 1, "value": 1}).sort("value", -1)
    pocet_clanku = collection.count()
    kateg_pocty = list(res_sorted)
    podil = [kateg_pocty[0]['value']]
    kategorie = [kateg_pocty[0]['_id']]
    for i in range(1, len(kateg_pocty)):
        podil.append(kateg_pocty[i]['value'])
        kategorie.append(kateg_pocty[i]['_id'])
    print(podil)
    fig = px.pie(values=podil, names=kategorie)
    fig.show()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
