from collections import UserDict, UserList
import requests
import pandas as pd
import copy

class Incorporator:
    """A super class meant to give children classes:
        * standard data type conversion methods
        * dictionary of class instances by given key
        * algorithm that dynamically names attributes during ingestion

    Attributes:
        codeDict (dict): instance code returns associated object instance
        convDict (dict): DF column name returns given type conversion function
        nameDict (dict): DF column name given new column name
        exclList (list): DF column names given will be excluded
        codeIdx (str): DF column name for cls Dictionary key values
        nameIdx (str): DF column name for object instance name values

    Methods:
        displayInfo (self): returns space formatted instance code and name
        getOrCreate (cls): returns dictionary value or creates new instance
        cnvattr (cls): converts incoming value by convDict result
        nextUrlREST (static): Get next API URL from JSON
        refreshDataREST (cls): Return dictionary of objects from JSON
    """

    ##TODO separate conv, update logic from JSON DF parsing
    ##TODO Formatted DisplayInfo with Details of all class attributes



    codeDict = UserDict()
    convDict = UserDict()
    nameDict = UserDict()
    exclLst  = UserList()
    codeIdx  = ''
    nameIdx  = ''

    def __init__(self, code, name=""):
        # Storing the idx code and name
        self.code = code
        self.name = name

    def __str__(self):
        return f"{self.code} - {self.name}"

    ## Formatted Code and Name print for visual checks
    def displayInfo(self,detailFlg=False):  # method without self parameter
        if detailFlg:
            print(vars(self))
        else:
            print(f"Code: {str(self.code).rjust(5, ' ')} Name: {self.name.ljust(20, ' ')}")

    ## Return known instance or create new one at dictionary key
    @classmethod
    def getOrCreate(cls, code, name):
        if code not in cls.codeDict:
            cls.codeDict.update({code: cls(code,name)})
        return cls.codeDict[code]

    ## Either convert value by convDict result or return unaltered value
    @classmethod
    def cnvattr(cls, attr):
        return lambda value: cls.convDict.get(attr, lambda value: value)(value)

    ## Rename DF column if value given in nameDict
    @classmethod
    def nameattr(cls, attr):
        return cls.nameDict.get(attr, attr)

    ## Return SubCls for data ingesttion
    @classmethod
    def incSubCls(
            cls, newSubCls, codeAttr, nameAttr, endpntAPI,
            codeAdds=None, exclAdds=None, convAdds=None, nameAdds=None
    ):
        newCodeDict = copy.deepcopy(cls.codeDict)
        newExclLst  = copy.deepcopy(cls.exclLst)
        newConvDict = copy.deepcopy(cls.convDict)
        newNameDict = copy.deepcopy(cls.nameDict)

        newCodeDict.update(codeAdds)
        newExclLst.extend(exclAdds)
        newConvDict.update(convAdds)
        newNameDict.update(nameAdds)
        return type(newSubCls, (cls, ),{'codeIdx': codeAttr, 'nameIdx': nameAttr, 'endpointAPI': endpntAPI,
            'codeDict': newCodeDict, 'exclLst': newExclLst, 'convDict': newConvDict, 'nameDict': newNameDict
            })

    ## Recursion through JSON dictionaries to Next URL value
    @staticmethod
    def nextUrlREST(jsonDict, keyPathLst):
        if len(keyPathLst) == 1:
            return jsonDict.get(keyPathLst[0],None)
        else:
            return Incorporator.nextUrlREST(jsonDict.get(keyPathLst[0], {}), keyPathLst[1:])

    ## Pop page as code from API URL
    @staticmethod
    def getCodeFromUrl(urlAPI):
        try:
            i = int(str(urlAPI).split("/").pop())
        except ValueError:
            i = urlAPI
        return i

    @classmethod
    def refreshDataREST(cls, nextUrl, rPath='results', nextUrlPath=None):
        while nextUrl:
            ## While API pages are avaliable loop through JSON Batches
            ## Use pandas DF to normalize batch, set Class code as index, remove exclList
            batch = requests.Session().get(nextUrl).json()
            batchDF = pd.json_normalize(batch, rPath, sep="_").set_index(cls.codeIdx).drop(columns=cls.exclLst)
            nextUrl = Incorporator.nextUrlREST(batch,nextUrlPath)

            ## Iterate Batch dict {code:name} to retrieve OR
            ## create missing Class instances
            defaultInstance = cls.getOrCreate(None, 'Null')
            batchDict = batchDF[cls.nameIdx].to_dict()
            for key, value in batchDict.items():
                cls.getOrCreate(key, value)

            ## Iterate DF columns to convert values
            ## Iterate DF dict of {code:row value} to update Class instances
            for col in batchDF.columns.values:
                attribDF = batchDF[col].apply(cls.cnvattr(col)).rename({col:cls.nameattr(col)}).to_dict()
                setattr(defaultInstance, cls.nameattr(col), "")
                for key, value in attribDF.items():
                    setattr(cls.codeDict[key], cls.nameattr(col), value)
        return cls.codeDict