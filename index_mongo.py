#-------------------------------------------------------------------------
# AUTHOR: Manson Pham
# FILENAME: index_mongo.py
# SPECIFICATION: demonstration PyMogo program
# FOR: CS 4250- Assignment #2
# TIME SPENT: 1 Hour
#-----------------------------------------------------------*/

from pymongo import MongoClient  # import mongo client to connect
from db_connection_mongo import *

if __name__ == '__main__':

    # Connecting to the database

    userHost = "localhost"
    userPort = 27017
    dbName = "CPP"
    db = connectDataBase(userHost, userPort, dbName)

    # Creating a collection
    documents = db.Documents

    
    option ="" 
    
    while option != "q":
        #print a menu
        print("")
        print("######### Menu ##############")
        print("#a - Create a document")
        print("#b - Update a document")
        print("#c - Delete a document.")
        print("#d - Output the inverted index.")
        print("#q - Quit")
        print("")
        
        option = input("Enter a menu choice: ")

        if (option == "a"):

            docId = input("Enter the ID of the document: ")
            docText = input("Enter the text of the document: ")
            docTitle = input("Enter the title of the document: ")
            docDate = input("Enter the date of the document: ")
            docCat = input("Enter the category of the document: ")
            print("")

            createDocument(documents, docId, docText, docTitle, docDate, docCat)

        elif (option == "b"):

            docId = input("Enter the ID of the document: ")
            docText = input("Enter the text of the document: ")
            docTitle = input("Enter the title of the document: ")
            docDate = input("Enter the date of the document: ")
            docCat = input("Enter the category of the document: ")
            print("")

            updateDocument(documents, docId, docText, docTitle, docDate, docCat)

        elif (option == "c"):

            docId = input("Enter the document id to be deleted: ")
            print("")

            deleteDocument(documents, docId)

        elif (option == "d"):
            print("")

            index = getIndex(documents)
            print(index)

        elif (option == "q"):

            print("Leaving the application ... ")

        else:

            print("Invalid Choice.")