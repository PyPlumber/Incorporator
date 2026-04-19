import requests

class IncorpJsonMixin:
    ## convert response to JSON for DF
    @staticmethod
    def sessionJSON(session):
        try:
            jsonData = session.json()
        except requests.exceptions.JSONDecodeError:
            print(f"DEBUGGING JSON DECODE ERROR, URL Called: {jsonUrl}")
            print(f"Status Code: {response.status_code}")
            print(f"Content-Type: {response.headers.get('Content-Type')}")
            raw_preview = response.text[:250] if response.text else "[Empty Response Body]"
            print(f"Raw Response Preview:\n{raw_preview}")
        return jsonData

    ## Recursion through JSON dictionaries to Next URL value
    @classmethod
    def nextUrlJSON(cls, jsonDict, keyPathLst):
        if keyPathLst is None:
            return None
        if len(keyPathLst) == 1:
            return jsonDict.get(keyPathLst[0],None)
        else:
            return cls.nextUrlJSON(jsonDict.get(keyPathLst[0], {}), keyPathLst[1:])

