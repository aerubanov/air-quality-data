import httpx
from bs4 import BeautifulSoup
import re
from multiprocessing.pool import ThreadPool


class FTPLoader:

    def __init__(self) -> None:
        self.base_url = "https://archive.sensor.community/"
        self.link_pattern = re.compile('\d\d\d\d-\d\d-\d\d/')

    def build_index(self):
        self.index = []
        resp = httpx.get(self.base_url)
        soup = BeautifulSoup(resp.text, 'lxml')
        links = [link.get('href') for link in soup.find_all('a')]
        pool = ThreadPool(processes=10)
        #TODO: remove links debug limit
        for result in map(self.check_and_scan_deeper, links):
            if result:
                self.index.extend(result)
        print(len(self.index))

    def check_and_scan_deeper(self, link):
        print(link)
        if self.link_pattern.match(link):
            url = self.base_url + link
            resp = httpx.get(url)
            soup = BeautifulSoup(resp.text, 'lxml')
            links = [item.get('href') for item in soup.find_all('a')]
            files = [item for item in links if '.csv' in item]
            return [url+i for i in files]

    def load_data(self):
        raise NotImplementedError


if __name__ == "__main__":
    loader = FTPLoader()
    loader.build_index()
