#-------------------------------------------------------------------------
# AUTHOR: Jason Phan
# FILENAME: db_connection_mongo
# SPECIFICATION: Methods to edit collection
# FOR: CS 4250- Assignment #2
# TIME SPENT: 1 hrs
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import pymongo

def connectDataBase():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    return mydb

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    split_text = docText.split()
    counts = {}
    for x in split_text:
        word = x.lower()
        if word not in counts:
            counts[word] = 0
        counts[word] += 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    docTerms = []
    for key, value in counts.items():
        term = key
        count = value
        num_chars = len(term)
        docTerms.append({"term": term, "count": count, "num_chars": num_chars})

    #Producing a final document as a dictionary including all the required fields
    dict = { "_id": docId, "text": docText, "title": docTitle, "date": docDate, "category": docCat, "terms": docTerms}

    # Insert the document
    col.insert_one(dict)


def deleteDocument(col, docId):

    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    cur = col.find({}, {"title":1, "terms":1, "_id":0})
    output = {}

    for doc in cur:
        title = doc["title"]
        terms = doc["terms"]
        for x in terms:
            term = x["term"]
            count = x["count"]
            if term not in output:
                output[term] = []
            output[term].append(title+':'+str(count))
    return output