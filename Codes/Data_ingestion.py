class Data_ingestion:
    pass

    # Delete the name and ... from the attributes for GDPR
    def __init__(self):
        self.path = None
        self.data = None

    def rmv_sensitive(self, data):
        self.data = data
        data1 = self.data.drop(['id', 'name', 'email', 'address',
                                'phone', 'location'], axis=1)
        try:
            data1.rename(columns={'exp': 'label'}, inplace=True)
        except:
            pass
        # data_dict = data.to_dict("records")
        return data1

    def rmv_sensitive_csv(self, data):
        self.data = data
        data1 = self.data.drop(['ID', 'Firstname', 'Surname',
                                'Email', 'Urban classification', 'Location'], axis=1)
        try:
            data1.rename(columns={'Expenditure': 'label'}, inplace=True)
        except:
            pass
        return data1

    def nd_integrated_ingestion(self, path, data_handler):
        self.path = path
        new_file_format = path.split(".")[-1]
        if new_file_format == "csv":
            print("File type is CSV [Using CSV module to handle the data]")
            new_file_data = data_handler.file_handler_csv(self.path)
            new_ingested_data = self.rmv_sensitive_csv(data=new_file_data)
        elif new_file_format == "json":
            print("File type is JSON [Using JSON module to handle the data]")
            new_file_data = data_handler.file_handler_json(self.path)
            new_ingested_data = self.rmv_sensitive(data=new_file_data)
        else:
            print("File format invalid")

        return new_ingested_data
