# -*- coding: utf-8 -*-
"""
## Install and load packages
"""

! pip install Faker
! pip install unidecode

from faker import Faker
import numpy as np
import unidecode
import json

"""## Initialize Faker Generator

"""

fake = Faker()

"""**Faker Localization** allows users to specify data for which location they need Faker package to return. """

#faker = Faker('en_GB')
faker = Faker('de')

from enum import IntEnum
#Define function to generate fake data and store into a JSON file
def generate_data(RECORD_COUNT):
    #Declare an empty dictionary
    my_fake_data ={}
    #Iterate the loop based on the input value and generate fake data
    for n in range(0, RECORD_COUNT):
        my_fake_data[n]={}
        my_fake_data[n]['id']= fake.random_number(digits=5)

        # Gender
        gender =  np.random.choice(["M", "F", "O"], p=[0.4, 0.5, 0.1])
        my_fake_data[n]['sex']= gender

        # Name 
        if gender=='M':
            first_name = faker.first_name_male()
        elif gender=='F':
            first_name = faker.first_name_female()
        else:
            first_name = faker.first_name_nonbinary()
        last_name = faker.last_name()
        my_fake_data[n]['name']= first_name + ' ' + last_name

        # Email address
        my_fake_data[n]['email']= unidecode.unidecode("{}.{}@{}".format(first_name, last_name, faker.free_email_domain()).lower())

        # Age
        my_fake_data[n]['age']= faker.pyint(min_value=18, max_value=80)

        # Working status
        if my_fake_data[n]['age'] > 65:
            work_status = np.random.choice(['working', 'not working', 'retired'], p=[0.05, 0.25, 0.7])  
        else:
            work_status = np.random.choice([ "student", "working", "not working"]) 
        my_fake_data[n]['working_status'] = work_status

        # Address
        my_fake_data[n]['address']= faker.address()

        # Phone Number
        my_fake_data[n]['phone'] = str(faker.phone_number()) 

        # Number of children                                                 #0   #1   #2   #3    #4     #5    #6     #7     #8      #9     #10
        my_fake_data[n]['n_children'] = int(np.random.choice(range(0,11), p=[0.2, 0.3, 0.3, 0.15, 0.025, 0.01, 0.006, 0.005, 0.0025, 0.001, 0.0005]))

        # Income
        # DE
        
        if work_status == 'student' or work_status == 'not working':   # limitation of income for student and not working
            income = faker.pyint(min_value=0, max_value=1000)
        elif work_status == 'retired':
            income = faker.pyint(min_value=5200, max_value= 150000)
        else:
            income = faker.pyint(min_value= 19500, max_value= 200000)
        my_fake_data[n]['income_annual'] = income

        """
        # UK
        if work_status == 'student' or work_status == 'not working':   # limitation of income for student and not working
            income = faker.pyint(min_value=0, max_value=3600)
        elif work_status == 'retired' :
            income = faker.pyint(min_value=8600, max_value= 100000)
        else:
            income = faker.pyint(min_value=12000, max_value= 200000)
        my_fake_data[n]['income_annual'] = income
        """

        # Location
        my_fake_data[n]['location'] = faker.city()

        # Home owned or not
        my_fake_data[n]['own_home'] = np.random.choice(["yes", "not"])

        # Marital status
        my_fake_data[n]['marital_status'] = np.random.choice( ["single", "married", "relationship", "widowed"] )
        
        # Technology exp
        rate = np.random.uniform(0, 0.8)
        my_fake_data[n]['exp'] = int(round(income*rate))
        
    #Write the data into the JSON file
    with open('my_fake_data_de.json', 'w') as fp:
        json.dump(my_fake_data, fp)
 
    print("File has been created.")
 
#Take the number of records from the user
num = int(input("Enter the number of records:"))
#Call the function to generate fake records and store into a json file
generate_data(num)

cat my_fake_data2.json
