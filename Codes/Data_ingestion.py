class Data_ingestion:
    pass

    # Delete the name and ... from the attributes for GDPR
    def __init__(self):
        self.data = None

    def rmv_sensitive(self, data):
        self.data = data
        data1 = self.data.drop(['id', 'name', 'email', 'address',
                                'phone', 'location'], axis=1)
        data1.rename(columns={'exp': 'label'}, inplace=True)
        # data_dict = data.to_dict("records")
        return data1

    def nd_rmv_sensitive(self, data):
        self.data = data
        data1 = self.data.drop(['id', 'name', 'email', 'address',
                                'phone', 'location'], axis=1)
        return data1

    def rmv_sensitive_csv(self, data):
        self.data = data
        data1 = self.data.drop(['ID', 'Firstname', 'Surname',
                                'Email', 'Urban classification', 'Location'], axis=1)
        data1.rename(columns={'Expenditure': 'label'}, inplace=True)
        return data1

    def nd_rmv_sensitive_csv(self, data):
        self.data = data
        data1 = self.data.drop(['ID', 'Firstname', 'Surname',
                                'Email', 'Urban classification', 'Location'], axis=1)
        return data1
