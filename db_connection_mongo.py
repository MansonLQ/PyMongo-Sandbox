#-------------------------------------------------------------------------
# AUTHOR: Manson Pham
# FILENAME: index_mongo.py
# SPECIFICATION: demonstration PyMogo program
# FOR: CS 4250- Assignment #2
# TIME SPENT: 1 Hour
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
from datetime import datetime

def connectDataBase(userHost, userPort, dbName):

    # Create a database connection object using pymongo
    # --> add your Python code here
    
    try:
        client = MongoClient(host=userHost, port=userPort)
        db = client[dbName]
        print("Database connected successfully!")
        return db

    except:
        print("Database not connected successfully")

def createDocument(collection, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    dictionary = {}
    
    removeChars = [';', ':', '!', "*", "."]
    filteredDocText = docText
    for i in removeChars:
        filteredDocText = filteredDocText.replace(i, '')
        
    num_chars = len(filteredDocText)
    
    wordList = filteredDocText.lower().split()
    for word in wordList:
        if (dictionary.get(word)):
            dictionary[word] += 1
        else:
            dictionary[word] = 1

    # create a list of dictionaries to include term objects. [{"term", count, num_char}]
    # --> add your Python code here
    termList = []
    for term in dictionary:
        termList.append({
            "term": term,
            "count": dictionary[term],
            "num_char": len(term)
        })
    

    #Producing a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "doc": int(docId),
        "category": docCat,
        "text": docText,
        "title": docTitle,
        "num_chars": num_chars,
        "date": datetime.strptime(docDate, "%Y-%m-%d"),
        "documentTerms": termList,
    }

    # Insert the document
    # --> add your Python code here
    try: 
        post_id = collection.insert_one(document).inserted_id
        print("Successfully inserted document with char count:", num_chars)    
    except: 
        print("Insert was unsuccessful")    
        return False
    
    

    return True

def deleteDocument(collection, docId):

    # Delete the document from the database
    # --> add your Python code here
    
    query = { "doc": docId }
    
    try: 
        collection.delete_one(query)
        print("Successfully removed document with Id:", docId)    
    except: 
        print("Delete was unsuccessful")    
        return False
    
    return True

def updateDocument(collection, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    
    deleteBoolean = deleteDocument(collection, docId)

    # Create the document with the same id
    # --> add your Python code here
    
    createBoolean = createDocument(collection, docId, docText, docTitle, docDate, docCat)
    
    return createBoolean and deleteBoolean

def getIndex(collection):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    

    pipeline = [
        { 
            '$unwind': '$documentTerms' 
        },
        { 
            '$group': {
                '_id': '$documentTerms.term',
                'occurrences': { 
                    '$push': { 
                        'docTitle': '$title', 
                        'termCount': '$documentTerms.count' 
                    } 
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'occurrences': {
                    '$reduce': {
                        'in': {
                            '$concat': [
                                '$$value',
                                {
                                    '$cond': {
                                        'else': ',',
                                        'if': { '$eq': ['$$value', ''] },
                                        'then': ''
                                    }
                                },
                                { '$concat': ['$$this.docTitle', ':', { '$toString': '$$this.termCount' }] }
                            ]
                        },
                        'initialValue': '',
                        'input': '$occurrences'
                    }
                },
                'term': '$_id'
            }
        },
        {
            '$sort': {
                'term': 1  # Sort terms alphabetically
            }
        },
        {
            '$group': {
                '_id': None,
                'terms': {
                    '$push': {
                        'k': '$term',
                        'v': '$occurrences'
                    }
                }
            }
        },
        {
            '$replaceRoot': {
                'newRoot': { '$arrayToObject': '$terms' }
            }
        }
    ]


    result = list(collection.aggregate(pipeline))
    if len(result) == 0:
        return "{}"
    else:
        return result[0]