import json
import pandas as pd
import pymongo


class Data_handler:
    def __init__(self):
        self.path = None

    def file_handler_json(self, path):
        # Handel the file resource data
        # read the file
        self.path = path
        file_data = pd.read_json(path, orient="index")
        return file_data

    def sql_handler(self):
        pass

    class Mongo_handler:

        def __init__(self):
            self.mongo_finds = None
            self.collection = None
            self.db_user_name = None
            self.db_pass = None

        def connect_db(self, db_user_name, db_pass):
            self.db_user_name = db_user_name
            self.db_pass = db_pass

            client = pymongo.MongoClient(
                "mongodb://" + self.db_user_name + ":" + self.db_pass +
                "@cluster0-shard-00-00.lb4p8.mongodb.net:27017,cluster0"
                "-shard-00-01.lb4p8.mongodb.net:27017,cluster0-shard-00-02."
                "lb4p8.mongodb.net:27017/egonetDatabase?ssl=true&replicaSet=atlas"
                "-3y5iws-shard-0&authSource=admin&retryWrites=true&w=majority")
            db = client.bdt_database
            collection = db.demog_data
            print("connection to the DB has been established: " + str(collection))
            return collection

        def insert_data(self, collection, file_data):
            # This method use for insert the arrived data to the MongoDB
            try:
                self.collection = collection
                collection.insert_many(file_data)
                print("Data inserted into database")
            except:
                print("Data insertion had problem")

        def find_data(self, collection):
            self.collection = collection
            fetched_data = collection.find()
            return fetched_data

        def model_prepared_mongo(self, mongo_finds):
            # New processes can be added to this method based on the structure of the data
            # Here the "_id" column will be dropped from the data frame [We do not need that for
            # our model].
            self.mongo_finds = mongo_finds
            list_cur = list(mongo_finds)
            data = pd.DataFrame(list_cur)
            data = data.drop("_id", axis=1)
            return data