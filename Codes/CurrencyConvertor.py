"""
## Install and load packages
"""
! pip install requests 

import requests

""" Create a class for currency convertion """

class CurrencyConverter:
    def __init__(self, url):
        self.data = requests.get(url).json()
        self.currencies = self.data['rates']

    def convert(self,from_currency, to_currency, amount):
        initial_amount = amount 
        if from_currency != 'EUR' : 
            amount = amount / self.currencies[from_currency] 
  
        # limiting the precision to 4 decimal places 
        amount = round(amount * self.currencies[to_currency], 4) 
        return amount
    
# the real-time exchange rates API with base currency EUR
url = 'https://api.exchangerate-api.com/v4/latest/EUR'

converter = CurrencyConverter(url)
