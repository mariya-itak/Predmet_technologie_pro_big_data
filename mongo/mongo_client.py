import pprint

from bson import Code
from pymongo import MongoClient
from pymongo.collation import Collation

client = MongoClient('localhost', 27017)
db = client.cv03
collection = db.idnes

#dotaz na vypsani vsech nazvu

def print_documents(query):
	for item in query:
		print(item)

print_documents(collection.find({}, {"_id":0, "name":1}).limit(10))

print("------------------------------------")

print(collection.count())

print("------------------------------------")

pipelin = [
	{"$group": {"_id": "$name", "count": {"$sum": 1}}},
	{"$sort": {"count": -1}},
	{"$limit": 10}
]
pprint.pprint(list(collection.aggregate(pipelin)))

print("------------------------------------")

print_documents(collection.find({}, {"_id": 0, "name": 1, "comments": 1}).sort("comments", -1).collation(Collation(locale='en_US', numericOrdering=True)).limit(1))

print("------------------------------------")

print_documents(collection.find({}, {"_id": 0, "photos": 1}).sort("photos", -1).collation(Collation(locale='en_US', numericOrdering=True)).limit(1))

print("------------------------------------")

mapper = Code("""function() {emit( this.date.substring(0, 4), 1)}""")
reducer = Code("""function(key, values){ return Array.sum(values);}""")
result = collection.map_reduce(mapper, reducer, "myResult")
res_sorted = result.find({}, {"date": 1, "value": 1}).sort("value", -1)
[pprint.pprint(doc) for doc in res_sorted]

print("------------------------------------")

mapper = Code("""function() {emit( this.category, 1)}""")
reducer = Code("""function(key, values){ return Array.sum(values);}""")
result = collection.map_reduce(mapper, reducer, "myResult")
res_sorted = result.find({}, {"date": 1, "value": 1}).sort("value", -1)
[pprint.pprint(doc) for doc in res_sorted]
print("Počet kategorií: ", res_sorted.count())

print("------------------------------------")

mapper = Code("""
            function() {
               if(this.date.substring(0, 4) == "2021"){
               this.name.split(' ').forEach(function(z) {
                  emit(z.toLowerCase(), 1);
               });
               }
            }
            """)

reducer = Code("""
                function(key, values) {
                    return Array.sum(values);
                }    
           """)
result = collection.map_reduce(mapper, reducer, "words").find()
[pprint.pprint(doc) for doc in result.sort("value", -1).limit(5)]

print("------------------------------------")

mapper = Code("""
            function() {
               this.content.split(' ').forEach(function(z) {
                  if(z.length >= 4){
                     emit(z.toLowerCase(), 1);
                  }
               });
            }
            """)

reducer = Code("""
                function(key, values) {
                    return Array.sum(values);
                }    
           """)
result = collection.map_reduce(mapper, reducer, "words").find()
[pprint.pprint(doc) for doc in result.sort("value", -1).limit(8)]

print("------------------------------------")

print_documents(collection.find({"date": {'$ne': ''}}, {'_id': 0, 'date': 1}).sort("date", 1).limit(1))

print("------------------------------------")

mapper = Code("""function() {emit("Count of comment", parseInt(this.comments))}""")
reducer = Code("""function(key, values){return Array.sum(values);}""")
result = collection.map_reduce(mapper, reducer, "words").find()
[pprint.pprint(doc) for doc in result]

print("------------------------------------")

mapper = Code("""
            function() {
               var suma = 0;
               this.content.split(' ').forEach(function(z) {
                  if(z !== ''){
                     suma = suma + 1;
                  }
               });
               emit("Count of words", suma);
            }
            """)
reducer = Code("""function(key, values){return Array.sum(values);}""")
result = collection.map_reduce(mapper, reducer, "words").find()
[pprint.pprint(doc) for doc in result]

print("------------------------------------")

mapper = Code("""
            function() {
               var suma = 0
               this.content.split(' ').forEach(function(z) {
               	  z = z.toLowerCase();
               	  if(z.includes("covid-19")){
               	  	suma = suma + 1;
                  }
               });
               emit(this, suma);
            }
            """)
reducer = Code("""function(key, values){return values;}""")
result = collection.map_reduce(mapper, reducer, "words").find()
[pprint.pprint(doc) for doc in result.sort("value", -1).limit(3)]
