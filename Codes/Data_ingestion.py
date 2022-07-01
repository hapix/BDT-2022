class Data_ingestion:
    """ This class is for first stage of facing with data and will implement some
    constraints for using the data. Like delete the name and ... from the attributes [GDPR].
    Also, will delete the variables that are not useful for the prediction [based on the expert ideas]
    So far this method will support the data format that we have and aso can be developed
    easily for other structure of data"""

    def __init__(self):
        self.path = None
        self.data = None

    def rmv_sensitive(self, data):
        self.data = data
        # will delete the useless columns from data
        data1 = self.data.drop(['id', 'name', 'email', 'address',
                                'phone', 'location'], axis=1)
        try:
            # will change the name of the dependent variable to label
            data1.rename(columns={'exp': 'label'}, inplace=True)
        except:
            pass
        return data1

    def rmv_sensitive_csv(self, data):
        self.data = data
        # will delete the useless columns from data
        data1 = self.data.drop(['ID', 'Firstname', 'Surname',
                                'Email', 'Urban classification', 'Location'], axis=1)
        try:
            # will change the name of the dependent variable to label
            data1.rename(columns={'Expenditure': 'label'}, inplace=True)
        except:
            pass
        return data1

    # this the integrated method to use two above ingestion method at the same time
    def nd_integrated_ingestion(self, path, data_handler):
        self.path = path

        # Will check the format of the fil based on the file extension and will pass the file
        # to the right section of ingestion
        new_file_format = path.split(".")[-1]
        if new_file_format == "csv":
            print("File type is CSV [Using CSV module to handle the data]")
            # Call the file handler from the data handler class to handle the data
            new_file_data = data_handler.file_handler_csv(self.path)
            new_ingested_data = self.rmv_sensitive_csv(data=new_file_data)
        elif new_file_format == "json":
            print("File type is JSON [Using JSON module to handle the data]")
            # Call the file handler from the data handler class to handle the data
            new_file_data = data_handler.file_handler_json(self.path)
            new_ingested_data = self.rmv_sensitive(data=new_file_data)
        else:
            print("File format invalid")

        return new_ingested_data
