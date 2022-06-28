class Data_ingestion:
    pass

    # Delete the name and ... from the attributes for GDPR
    def __init__(self):
        self.data = None

    def rmv_sensitive(self, data):
        self.data = data
        data = data.drop(['id', 'name', 'email', 'address',
                          'phone', 'location'], axis=1)
        data.rename(columns={'exp': 'label'}, inplace=True)
        data_dict = data.to_dict("records")
        return data_dict

    def rmv_sensitive_csv(self, data):
        self.data = data
        data = data.drop(['ID', 'Firstname', 'Surname',
                          'Email', 'Urban classification', 'Location'], axis=1)
        data.rename(columns={'Expenditure': 'label'}, inplace=True)
        data_dict = data.to_dict("records")
        return data_dict
