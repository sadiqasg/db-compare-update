from pymongo import MongoClient
import time

# MongoDB connection details
old_db_uri = "mongodb://localhost:27017/gwappdb"
new_db_uri = "mongodb+srv://abu:9Pgc2Cn3Z4YsMvAe@maincluster.tbnbuz3.mongodb.net/gwappdb?retryWrites=true&w=majority"

# Connect to MongoDB clients
old_client = MongoClient(old_db_uri)
new_client = MongoClient(new_db_uri)

# Test connections
try:
    old_client.server_info()
    print("Connected to old MongoDB")
except Exception as e:
    print(f"Failed to connect to old MongoDB: {e}")

try:
    new_client.server_info()
    print("Connected to MongoDB Atlas")
except Exception as e:
    print(f"Failed to connect to MongoDB Atlas: {e}")

# Database and collection names
old_db = old_client.get_database("gwappdb")
new_db = new_client.get_database("gwappdb")

# Collection names
old_collection = old_db["requisitions"]
new_collection = new_db["requisitions"]

# Function to compare and find missing fields
def find_and_copy_missing_fields():
    start_time = time.time()
    missing_fields_count = 0
    updated_documents_count = 0
    total_old_documents = old_collection.count_documents({})
    total_new_documents = new_collection.count_documents({})

    print(f"Total documents in old collection: {total_old_documents}")
    print(f"Total documents in new collection: {total_new_documents}")

    if total_old_documents == 0:
        print("Old collection has no documents.")
        print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")
        return
    if total_new_documents == 0:
        print("New collection has no documents.")
        print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")
        return

    # Query all documents from old database
    query_start_time = time.time()
    old_documents = list(old_collection.find())
    print(f"Document Query Time: {time.time() - query_start_time:.2f} seconds")

    process_start_time = time.time()
    # Iterate through each document and check if it exists in new database
    for i, old_doc in enumerate(old_documents):
        title = old_doc.get("title")
        total = old_doc.get("total")
        name = old_doc.get("name")
        date = old_doc.get("date")

        # Query by title, amount, name, and date in new database
        matching_doc = new_collection.find_one({"title": title, "total": total, "name": name, "date": date})

        # If a matching document is found, check for missing fields
        if matching_doc:
            missing_fields = {field: value for field, value in old_doc.items() if field not in matching_doc}
            if missing_fields:
                missing_fields_count += 1
                # Update the new document with the missing fields from the old document
                new_collection.update_one(
                    {"_id": matching_doc["_id"]},
                    {"$set": missing_fields}
                )
                updated_documents_count += 1
        
        # Add progress logging every 100 documents
        if i % 100 == 0:
            print(f"Processed {i}/{total_old_documents} documents")

    print(f"Document Processing Time: {time.time() - process_start_time:.2f} seconds")
    print(f"No of docs with missing fields: {missing_fields_count} out of {total_old_documents}")
    print(f"No of updated docs in new db: {updated_documents_count}")
    print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")

# Execute the function to find and copy missing fields
find_and_copy_missing_fields()

# Close MongoDB connections
old_client.close()
new_client.close()





# INITIAL

# from pymongo import MongoClient
# import time

# # MongoDB connection details
# old_db_uri = "mongodb://localhost:27017/gwappdb"
# new_db_uri = "mongodb+srv://abu:9Pgc2Cn3Z4YsMvAe@maincluster.tbnbuz3.mongodb.net/gwappdb?retryWrites=true&w=majority"

# # Connect to MongoDB clients
# old_client = MongoClient(old_db_uri)
# new_client = MongoClient(new_db_uri)

# # Test connections
# try:
#     old_client.server_info()
#     print("Connected to old MongoDB")
# except Exception as e:
#     print(f"Failed to connect to old MongoDB: {e}")

# try:
#     new_client.server_info()
#     print("Connected to MongoDB Atlas")
# except Exception as e:
#     print(f"Failed to connect to MongoDB Atlas: {e}")

# # Database and collection names
# old_db = old_client.get_database("gwappdb")
# new_db = new_client.get_database("gwappdb")

# # Collection names
# old_collection = old_db["requisitions"]
# new_collection = new_db["requisitions"]

# # Function to compare and find missing fields
# def find_and_copy_missing_fields():
#     start_time = time.time()
#     missing_fields_count = 0
#     updated_documents_count = 0
#     total_old_documents = old_collection.count_documents({})
#     total_new_documents = new_collection.count_documents({})

#     print(f"Total documents in old collection: {total_old_documents}")
#     print(f"Total documents in new collection: {total_new_documents}")

#     if total_old_documents == 0:
#         print("Old collection has no documents.")
#         print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")
#         return
#     if total_new_documents == 0:
#         print("New collection has no documents.")
#         print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")
#         return

#     # Query all documents from old database
#     query_start_time = time.time()
#     old_documents = list(old_collection.find())
#     print(f"Document Query Time: {time.time() - query_start_time:.2f} seconds")

#     process_start_time = time.time()
#     # Iterate through each document and check if it exists in new database
#     for old_doc in old_documents:
#         title = old_doc.get("title")
#         total = old_doc.get("total")
#         name = old_doc.get("name")
#         date = old_doc.get("date")

#         # Query by title, amount, name, and date in new database
#         matching_doc = new_collection.find_one({"title": title, "total": total, "name": name, "date": date})

#         # If a matching document is found, check for missing fields
#         if matching_doc:
#             missing_fields = {field: value for field, value in old_doc.items() if field not in matching_doc}
#             if missing_fields:
#                 missing_fields_count += 1
#                 # Update the new document with the missing fields from the old document
#                 new_collection.update_one(
#                     {"_id": matching_doc["_id"]},
#                     {"$set": missing_fields}
#                 )
#                 updated_documents_count += 1

#     print(f"Document Processing Time: {time.time() - process_start_time:.2f} seconds")
#     print(f"No of docs with missing fields: {missing_fields_count} out of {total_old_documents}")
#     print(f"No of updated docs in new db: {updated_documents_count}")
#     print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")

# # Execute the function to find and copy missing fields
# find_and_copy_missing_fields()

# # Close MongoDB connections
# old_client.close()
# new_client.close()



# SECOND CODE

# from pymongo import MongoClient
# import time

# old_db_uri = "mongodb://localhost:27017/gwappdb"
# new_db_uri = "mongodb+srv://abu:9Pgc2Cn3Z4YsMvAe@maincluster.tbnbuz3.mongodb.net/gwappdb?retryWrites=true&w=majority"
# # new_db_uri = "mongodb://localhost:27017/newgwappdb"

# # Connect to MongoDB clients
# old_client = MongoClient(old_db_uri)
# new_client = MongoClient(new_db_uri)

# # Database and collection names
# old_db = old_client.get_database("gwappdb")
# new_db = new_client.get_database("newgwappdb")

# # Collection names
# old_collection = old_db["requisitions"]
# new_collection = new_db["requisitions"]

# # Function to compare and find missing fields
# def find_and_copy_missing_fields():
#     start_time = time.time()
#     missing_fields_count = 0
#     updated_documents_count = 0
#     total_old_documents = old_collection.count_documents({})
#     total_new_documents = new_collection.count_documents({})

#     print(f"Total documents in old collection: {total_old_documents}")
#     print(f"Total documents in new collection: {total_new_documents}")

#     if total_old_documents == 0:
#         print("Old collection has no documents.")
#         print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")
#         return
#     if total_new_documents == 0:
#         print("New collection has no documents.")
#         print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")
#         return

#     # Query all documents from old database
#     query_start_time = time.time()
#     old_documents = list(old_collection.find())
#     print(f"Document Query Time: {time.time() - query_start_time:.2f} seconds")

#     process_start_time = time.time()
#     # Iterate through each document and check if it exists in new database
#     for old_doc in old_documents:
#         title = old_doc.get("title")
#         total = old_doc.get("total")
#         name = old_doc.get("name")
#         date = old_doc.get("date")

#         # Query by title, amount, name, and date in new database
#         matching_doc = new_collection.find_one({"title": title, "total": total, "name": name, "date": date})

#         # If a matching document is found, check for missing fields
#         if matching_doc:
#             missing_fields = {field: value for field, value in old_doc.items() if field not in matching_doc}
#             if missing_fields:
#                 missing_fields_count += 1
#                 # print(f"Document with title={title}, amount={amount}, name={name}, date={date} is missing fields: {list(missing_fields.keys())}")
#                 # Update the new document with the missing fields from the old document
#                 new_collection.update_one(
#                     {"_id": matching_doc["_id"]},
#                     {"$set": missing_fields}
#                 )
#                 updated_documents_count += 1
#         # else:
#         #     print(f"Document with title={title}, amount={amount}, name={name}, date={date} is missing entirely in the new database.")

#     print(f"Document Processing Time: {time.time() - process_start_time:.2f} seconds")
#     print(f"No of docs with missing fields: {missing_fields_count} out of {total_old_documents}")
#     print(f"No of updated docs in new db: {updated_documents_count}")
#     print(f"Total Execution Time: {time.time() - start_time:.2f} seconds")

# # Execute the function to find and copy missing fields
# find_and_copy_missing_fields()

# # Close MongoDB connections
# old_client.close()
# new_client.close()



