import requests
import json

class ShopifyScraper():
    def __init__(self, baseurl):
        self.baseurl = baseurl

    def downloadJson(self, page):
        r = requests.get(self.baseurl + f'products.json?limit=250&page={page}', timeout=10)
        if r.status_code != 200:
            print(f"Error downloading page: {str(r.status_code)} {page}")
            return None

        data = r.json()
        if 'products' in data and len(data['products']) > 0:
            return data['products']
        else:
            print(f"No products found on page: {page}")
            return None
