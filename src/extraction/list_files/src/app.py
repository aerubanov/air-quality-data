import httpx
import bs4
import json

base_url = "https://archive.sensor.community/"

def lambda_handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    link = event["folder"]
    files = list_files(link)
    return json({"files": files})


def list_files(link):
    """
    Return list of hrefs from link
    """
    url = base_url + link
    # load url with retries
    try:
        response = httpx.get(url, timeout=20)
    except httpx.ReadTimeout:
        print("Timeout")
        response = httpx.get(url, timeout=120)

    soup = bs4.BeautifulSoup(response.text, 'lxml')
    links = [item.get('href') for item in soup.find_all('a')]
    files = [item for item in links if '.csv' in item]
    return [url+file for file in files]


if __name__ == "__main__":
    files = list_files(link='2015-10-01/')
    print(files)