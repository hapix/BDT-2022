import pandas as pd
import pymongo
import datetime


class Data_handler:
    """This class handel the data input and transferring data between the code and database.
    Also, it contains another class named Mongo_handler() for handling the database actions
    like read from or writ into the database"""

    def __init__(self):
        self.path = None

    # Handling the json format file
    def file_handler_json(self, path):
        # Handel the file resource data JSON
        # read the file
        self.path = path
        file_data = pd.read_json(self.path, orient="index")
        return file_data

    # Handling the CSV format file
    def file_handler_csv(self, path):
        # Handel the file resource data CSV
        # read the file
        self.path = path
        try:
            file_data = pd.read_csv(self.path)
        except:
            file_data = pd.read_csv(self.path, encoding="ISO-8859-1")
        return file_data

    # This class handel the different part of the interaction with database
    # Note : the database is on the cloud so there is a stable internet connection needed
    class Mongo_handler:

        def __init__(self):
            self.mongo_finds = None
            self.collection = None
            self.db_user_name = None
            self.db_pass = None

        # connection to the database. will return Database and Collection objects
        # Username and passwords are located to the main.py file.
        def connect_db(self, db_user_name, db_pass):
            self.db_user_name = db_user_name
            self.db_pass = db_pass

            # Make a mongo db client
            client = pymongo.MongoClient(
                "mongodb://" + self.db_user_name + ":" + self.db_pass +
                "@cluster0-shard-00-00.lb4p8.mongodb.net:27017,cluster0"
                "-shard-00-01.lb4p8.mongodb.net:27017,cluster0-shard-00-02."
                "lb4p8.mongodb.net:27017/egonetDatabase?ssl=true&replicaSet=atlas"
                "-3y5iws-shard-0&authSource=admin&retryWrites=true&w=majority")
            db = client.bdt_database  # connect to the DB
            collection = db.demog_data  # connect to the collection
            print("connection to the DB has been established: " + str(collection))
            return db, collection

        # This method use for insert the arrived data to the MongoDB
        # This method will get the collection and data [data would be the list of documents]
        # and will insert the data to the collection
        def insert_data(self, collection, file_data):
            utc_timestamp = datetime.datetime.utcnow()
            file_data1 = file_data.to_dict("records")
            self.collection = collection

            # Make an index for putting TTL index on the data for limitation of the storage
            # For default the TTL duration has been set for 3 months
            # The indexed key named "insert_data" will be updated for all the records
            self.collection.create_index("insert_date", expireAfterSeconds=13140 * 60)
            self.collection.insert_many(file_data1)
            self.collection.update_many({}, {"$set": {"insert_date": utc_timestamp}}, upsert=False)
            print("Data inserted into database")

        # Retrieve the all data fo collection
        def find_data(self, collection):
            self.collection = collection
            # there is no condition for finding the data on query because we want all the data
            fetched_data = collection.find()
            return fetched_data

        # Create a collection for saving the new data in the database
        # These collections have the same name with the made model i previously steps
        @staticmethod
        def create_collection(collection_name, database_name):
            new_collection = database_name[collection_name]
            return new_collection

        # For removing some useless columns from retrieving data from database
        # New processes can be added to this method based on the structure of the data
        # Here the "_id" column will be dropped from the data frame [We do not need that for
        # our model].
        def model_prepared_mongo(self, mongo_finds):
            self.mongo_finds = mongo_finds
            list_cur = list(mongo_finds)
            data = pd.DataFrame(list_cur)
            data = data.drop(["_id", "insert_date"], axis=1)
            return data

        # Retrieving the existence collection names
        @staticmethod
        def collection_names(db):
            return db.list_collection_names()
