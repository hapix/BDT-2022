"""
## Install and load packages
"""
! pip install faker
! pip install unidecode

import unidecode
import csv
import numpy as np
from faker import Faker

"""## Initialize Faker Generator
"""

fake = Faker()

"""**Faker Localization** allows users to specify data for which location they need Faker package to return. """
#faker = Faker('es_ES')
faker = Faker('it_IT')

RECORD_COUNT = 15000

#Define function to generate fake data and store into a CSV file
def writeTo_csv():
  with open('my_fake_data.csv', 'w', newline='') as csvfile:
    # Define the attributes
    fieldnames = ['ID', 'Firstname', 'Surname', 'Email', 'Age', 'Sex', "Education" , 'Urban classification', '#children', 
                  'Working status', 'Annual income', 'Marital status', 'Location', 'Ownership', 'Household', 'Expenditure']
    seed = 0
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    # Iterate the loop based on the input value and generate fake data
    for i in range(RECORD_COUNT):
        seed += 1
        # Sex
        gender =  np.random.choice(["M", "F", "O"], p=[0.4, 0.4, 0.2])
        
        # Name
        if gender=="M":
            first_name =  faker.first_name_male()
        if gender=="F":
            first_name =  faker.first_name_female()
        if gender=="O":
            first_name =  faker.first_name_nonbinary()
        
        # Firstname
        last_name = faker.last_name()
        
        # Number of children
        child = np.random.choice([0,1,2,3,4,5,6,7,8,9,10], p=[0.25, 0.2, 0.2, 0.15, 0.07, 0.05, 0.03, 0.02, 0.015, 0.01, 0.005 ])
        
        # Age
        age = faker.pyint(min_value=18, max_value=80)
        
        # Work status
        if age > 50:
            work_status = np.random.choice([ "student", "working", "not working", "retired"])
        else:
            work_status = np.random.choice([ "student", "working", "not working"])

        # Income
        if work_status == 'student' or work_status == 'not working':
            #income = faker.pyint(min_value=0, max_value=5000)         # ES
            income = faker.pyint(min_value=0, max_value=6000)          # ita
        if work_status == 'working':
            #income = faker.pyint(min_value=12000, max_value= 200000)  # ES
            income = faker.pyint(min_value=12600, max_value= 200000)   # ita 
        if work_status == 'retired':
            #income = faker.pyint(min_value=13000, max_value= 200000)  # ES
            income = faker.pyint(min_value=6800, max_value= 200000)   # ita 
        
        # Expenditure
        rate = np.random.uniform(0,0.8)
        exp = round(income * rate)


        # Write data into a CSV file
        writer.writerow({"ID": seed, 
                         "Firstname": first_name, 
                         "Surname": last_name,
                         
                         # Email address 
                         "Email": unidecode.unidecode("{}.{}@{}".format(first_name, last_name, faker.free_email_domain()).lower()),
                         
                         "Age": age ,  
                         "Sex": gender, 
                         
                         # Level of education
                         "Education": np.random.choice(["No", "Elementary", "Graduated"]),
                         # Urban classification
                         "Urban classification":  np.random.choice(["city", "town and suburb"," rural area"]),
                         
                         '#children': child, 
                         "Working status": work_status,
                         'Annual income': income, 
                         
                         # Marital status
                         'Marital status': np.random.choice( ["single", "married", "partnership", "widowed"]),
                         
                         # Location according to faker localization
                         "Location": faker.city(), 
                         
                         # Ownership
                         "Ownership":  np.random.choice([ "owner", "rent"]), 
                         
                         # Hosehold
                         "Household": np.random.choice([ "single", "multi"]),
                         
                         "Expenditure":exp})
 


#Call the function to generate fake records and store into a json file
writeTo_csv()



