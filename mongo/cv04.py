import pprint

from bson import Code
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.cv03
collection = db.idnes

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pymongo.collation import Collation

def print_documents(query):
	for item in query:
		print(item)


def task7():
    mapper = Code("""
                function() {
                   title = this.name
                   this.content.split(' ').forEach(function(z) {
                      emit(title, 1);
                   });
                }
                """)

    reducer = Code("""
                    function(key, values) {
                        return Array.sum(values);
                    }    
               """)
    result = collection.map_reduce(mapper, reducer, "Count of words in article text")
    values = []
    for article in list(result.find({}, {"value": 1, "_id": 0})):
        values.append(article['value'])
    fig = px.histogram(values)
    fig.show()


def task8():
    mapper = Code("""
                     function() {
                        this.content.split(' ').forEach(function(z) {
                           z = z.replace(/^\s+|\s+$/g, '');
                           if(z.length !== 0){
                             emit(z.length, 1);
                           }
                        });
                     }
                     """)

    reducer = Code("""
                         function(key, values) {
                             return Array.sum(values);
                         }
                    """)
    result = collection.map_reduce(mapper, reducer, "Count of words in article text")
    length_word = []
    count_occurrences = []
    #[pprint.pprint(doc) for doc in result.find()]
    for article in list(result.find()):
     count_occurrences.append(article['value'])
     length_word.append(article["_id"])
    #length_word=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0, 49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0, 76.0, 78.0, 79.0, 81.0, 82.0, 86.0, 90.0, 92.0, 93.0, 94.0, 97.0, 99.0, 101.0, 102.0, 104.0, 113.0, 134.0, 145.0, 186.0, 189.0, 195.0, 221.0, 267.0, 269.0, 272.0, 380.0, 385.0, 391.0, 507.0, 517.0, 878.0]
    #count_occurrences=[2919433.0, 4414213.0, 2250054.0, 3410325.0, 4370210.0, 3798268.0, 3671324.0, 3260133.0, 2265143.0, 1540940.0, 902282.0, 546685.0, 346345.0, 232808.0, 216276.0, 134189.0, 117054.0, 71789.0, 62657.0, 39012.0, 33156.0, 34540.0, 24828.0, 15889.0, 13221.0, 10568.0, 12907.0, 7631.0, 4930.0, 4738.0, 3747.0, 1508.0, 946.0, 638.0, 407.0, 328.0, 4244.0, 181.0, 124.0, 78.0, 72.0, 72.0, 40.0, 1612.0, 23.0, 21.0, 20.0, 17.0, 15.0, 13.0, 1511.0, 2292.0, 8.0, 4.0, 7.0, 3.0, 5.0, 4.0, 4.0, 5.0, 5.0, 2.0, 3.0, 4.0, 6.0, 2.0, 1.0, 2.0, 2.0, 1.0, 3.0, 1.0, 3.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0]
    #print(length_word)
    #print(count_occurrences)
    fig = px.histogram(x=length_word, y=count_occurrences, nbins=1000)
    fig.show()


def task9():
    mapper = Code("""
                function() {
                   var suma = 0
                   this.content.split(' ').forEach(function(z) {
                   	  z = z.toLowerCase();
                   	  if(z.includes("koronavirus")){
                   	  	suma = suma + 1;
                      }
                   });
                   emit(this.date, suma);
                }
                """)
    reducer = Code("""function(key, values){return Array.sum(values);}""")
    results = collection.map_reduce(mapper, reducer, "Statistics of word covid").find({"_id": {'$ne': ''}}).sort("_id", 1)
    dates_covid = []
    count_covid = []
    for doc in results:
        dates_covid.append(doc["_id"])
        count_covid.append(int(doc['value']))

    mapper = Code("""
                    function() {
                       var suma = 0
                       this.content.split(' ').forEach(function(z) {
                       	  z = z.toLowerCase();
                       	  if(z.includes("vakcína")){
                       	  	suma = suma + 1;
                          }
                       });
                       emit(this.date, suma);
                    }
                    """)
    reducer = Code("""function(key, values){return Array.sum(values);}""")
    results = collection.map_reduce(mapper, reducer, "Statistics of word covid").find({"_id": {'$ne': ''}}).sort("_id", 1)
    dates_vakcina = []
    count_vakcina = []
    for doc in results:
        dates_vakcina.append(doc["_id"])
        count_vakcina.append(int(doc['value']))
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates_covid, y=count_covid, mode='lines+markers', name='covid'))
    fig.add_trace(go.Scatter(x=dates_vakcina, y=count_vakcina, mode='lines+markers', name='vakcína'))
    fig.show()


def task10():
    mapper = Code("""
                    function() {
                       if(this.date.substring(0, 10) !== ''){
                          emit(this.date.substring(0, 10), 1);
                       }
                    }
                    """)
    reducer = Code("""function(key, values){return Array.sum(values);}""")
    results = collection.map_reduce(mapper, reducer, "Statistics of word covid").find({"_id": {'$ne': ''}}).sort("_id", 1)

    dates = []
    count = []
    suma = 0
    for doc in results:
        dates.append(doc["_id"])
        count.append(doc['value'])
        suma += doc['value']
    fig = px.histogram(x=dates, y=count, nbins=100)
    fig.show()


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
    fig.show()

    fig = px.bar(x=year, y=count)
    fig.show()

    result = collection.find({}, {"_id": 0, "content": 1, "comments": 1}).sort("comments", -1).collation(Collation(locale='en_US', numericOrdering=True)).limit(1000)
    df = pd.DataFrame(data=result)
    for i in range(0, len(df['content'])):
        df['content'][i] = len(str(df['content'][i]).split(' '))
    fig = px.scatter(x=df['content'], y=df['comments'])
    fig.show()

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
    df = pd.DataFrame({"podil": podil, "kategorie": kategorie})
    fig = px.pie(df, values="podil", names="kategorie")
    fig.show()

    task7()
    task8()
    task9()
    task10()