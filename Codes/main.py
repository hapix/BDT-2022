# Big data project | Hamid, Elisa, Elisa
from Data_handler import Data_handler
from Model_maker import Model_maker
from Data_ingestion import Data_ingestion
from API_handler import API_handler
import art

if __name__ == '__main__':
    db_user_name = "MoonElisaHamid"
    db_pass = "HamidElisaMoon"
    # path = "C:\\Users\\hamid\\Downloads\\my_fake_data_uk.json"
    # path = "C:\\Users\\hamid\\Downloads\\my_fake_data_it.csv"
    p_model_path = "D:\\Unitn\\BigData\\p_models"
    ml_model_path = "D:\\Unitn\\BigData\\ml_models"

    data_handler = Data_handler()
    data_ingestion = Data_ingestion()
    model_maker = Model_maker()

    # Banner
    art.tprint('Techno  Pred', "tarty1")
    print("Welcome to Techno Pred. Estimate your yearly expenditure o technology!")
    (db, collection) = data_handler.Mongo_handler().connect_db(db_user_name=db_user_name, db_pass=db_pass)

    while True:
        print("\nChoose an option [Enter the number]:\n")
        print("[1] Make a new model.")
        print("[2] Estimate the expenditure based on a predefined model.")
        print("[3] Exit.")
        first_step = input()

        if first_step == "1":
            print("Enter the path of the data:")
            # path = input()
            path = "C:\\Users\\hamid\\Downloads\\my_fake_data_it.csv"
            ingested_data = data_ingestion.nd_integrated_ingestion(path=path, data_handler=data_handler)

            print("Choose a name for the model: ")
            collection_name = input()
            existence_collections = data_handler.Mongo_handler().collection_names(db)
            while collection_name in existence_collections:
                print("The name is existed. Please choose a new name:")
                collection_name = input()

            new_collection = data_handler.Mongo_handler().create_collection(collection_name=collection_name,
                                                                            database_name=db)
            data_handler.Mongo_handler().insert_data(collection=new_collection, file_data=ingested_data)
            #  Spark initialization
            (spark, sc) = model_maker.spark_init()
            (pipeline_model, ml_model, refine_data) = model_maker.set_model(data=ingested_data, spark=spark)
            model_maker.save_p_model(p_model=pipeline_model, p_model_path=p_model_path + "\\" + collection_name)
            model_maker.save_ml_model(ml_model=ml_model, ml_model_path=ml_model_path + "\\" + collection_name)

            print("Enter the path of the new data to Estimate:\n ")
            # new_data_path = input()
            new_data_path = "C:\\Users\\hamid\\Downloads\\new_data_it.csv"
            new_ingested_data = data_ingestion.nd_integrated_ingestion(path=new_data_path, data_handler=data_handler)
            (new_pipeline_model, new_ml_model, new_refine_data) = model_maker.set_model(data=new_ingested_data,
                                                                                        spark=spark,
                                                                                        loaded_p_model=pipeline_model)
            predicted_df = model_maker.prediction_module(refine_data=new_refine_data, ml_model=ml_model)
            for curr in predicted_df["prediction"]:
                print(curr, API_handler().convert("EUR", "GBP", curr))

        elif first_step == "2":
            (spark, sc) = model_maker.spark_init()
            print("Choose one of the models:")
            existence_collections = data_handler.Mongo_handler().collection_names(db)
            for item_num, item_model in enumerate(existence_collections):
                print(f"[{item_num + 1}] {item_model}")
            selected_model = int(input())

            # bargardoondane forate column ha ba tavajoh be model
            selected_model_name = existence_collections[selected_model - 1]
            print(f"Model \'" + selected_model_name + "\' has been selected")
            loaded_p_model = model_maker.load_p_model(p_model_path + "\\" + selected_model_name)
            loaded_ml_model = model_maker.load_ml_model(ml_model_path + "\\" + selected_model_name)
            print("Enter the path of the new data to Estimate:\n ")
            # new_data_path = input()
            new_data_path = "C:\\Users\\hamid\\Downloads\\new_data_it.csv"
            new_ingested_data = data_ingestion.nd_integrated_ingestion(path=new_data_path,data_handler=data_handler)
            (new_pipeline_model, new_ml_model, new_refine_data) = model_maker.set_model(data=new_ingested_data, spark=spark,
                                                                                        loaded_p_model=loaded_p_model)
            predicted_df = model_maker.prediction_module(refine_data=new_refine_data, ml_model=loaded_ml_model)

            for curr in predicted_df["prediction"]:
                print(curr, API_handler().convert("EUR", "GBP", curr))

        elif first_step == "3":
            break

        else:
            continue

    sc.stop()
