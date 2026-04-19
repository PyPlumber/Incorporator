import requests
import re
from urllib3 import Retry
from requests.adapters import HTTPAdapter

class IncorpApiMixin:
    ## Retry Strategy for long API pulls
    @staticmethod
    def retryAPI(session, retryUrl, backoffFactor=1.0):
        retryControls = Retry(
            total=5,
            backoff_factor=backoffFactor,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        session.mount("https://", HTTPAdapter(max_retries=retryControls))
        return session.get(retryUrl)

    ## test API response
    @staticmethod
    def sessionAPI(session, url):
        try:
            response = IncorpApiMixin.retryAPI(session, url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP Error occurred: {http_err} URL: {url}")
        except requests.exceptions.ConnectionError:
            print("Network Error: All retries exhausted, still cannot connect.")
        except requests.exceptions.RequestException as e:
            print(f"An unexpected network error occurred: {e}")
        return response

    ## Get page as code from URL, consider lists and trail slash
    @staticmethod
    def getCodeFromUrlAPI(urlAPI, position=0):
        urlPattern = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
        urlList  = re.findall(urlPattern, urlAPI)
        urlList  = [re.sub("^/|/$", "", i) for i in urlList]
        codeList = [i.split('/')[-1] for i in urlList]
        try:
            cd = int(codeList[position])
        except (ValueError, IndexError):
            cd = urlAPI
        return cd



