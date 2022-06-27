# Big data project | Hamid, Elisa, Elisa
import pyspark
from Data_handler import Data_handler
from Model_maker import Model_maker
from API_handler import API_handler
from Data_ingestion import Data_ingestion
if __name__ == '__main__':
    db_user_name = "MoonElisaHamid"
    db_pass = "HamidElisaMoon"
    path = "C:\\Users\\hamid\\Downloads\\my_fake_data_uk.json"

    # Import the data

    data_handler = Data_handler()
    # data_handler = Data_handler.Mongo_handler()  # class initialization
    data_ingestion = Data_ingestion()

    file_data = data_handler.file_handler_json(path=path)
    ingested_data = data_ingestion.rmv_sensitive(data=file_data)
    collection = data_handler.Mongo_handler().connect_db(db_user_name=db_user_name, db_pass=db_pass)
    # data_handler.Mongo_handler().insert_data(collection=collection, file_data=ingested_data)

    all_data = data_handler.Mongo_handler().find_data(collection)
    prepared_data = data_handler.Mongo_handler().model_prepared_mongo(mongo_finds=all_data)
    # print(prepared_data)
    # print(type(prepared_data))

    model_maker = Model_maker()
    model_maker.set_model(prepared_data)
    # api_handler = API_handler()
    #
    # model_maker.set_model()
    # model_maker.save_model()
    #
    # api_handler.Ex_api()


    # data in the data frame for analyzes
