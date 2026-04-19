import requests
import pandas as pd
import copy
from datetime import date
from dateutil.parser import parse, ParserError

from Incorporator.methods.api_utils import IncorpApiMixin
from Incorporator.methods.json_utils import IncorpJsonMixin
from Incorporator.methods.dict_utils import IncorpDictMixin

class Incorporator(IncorpApiMixin, IncorpJsonMixin, IncorpDictMixin):
    """A super class meant to give children classes:
        * standard data type conversion methods
        * dictionary of class instances by given key
        * attributes that act as pointers to related Class instances
        * algorithm that dynamically names attributes during ingestion

    Attributes:
        codeDict (dict): instance code returns associated object instance
        convDict (dict): DF column name returns given type conversion function
        nameDict (dict): DF column name given new column name
        exclLst (list): DF column names given will be excluded
        codeIdx (str): DF column name for cls Dictionary key values
        nameIdx (str): DF column name for object instance name values

    Methods:
        displayInfo (self): returns space formatted instance code and name
        getOrCreate (cls): returns dictionary value or creates new instance
        cnvattr (cls): converts incoming value by convDict result
        nextUrlREST (static): Get next API URL from JSON
        refreshDataREST (cls): Return dictionary of objects from JSON
    """
    codeDict = dict()
    convDict = dict()
    nameDict = dict()
    exclLst  = list()
    codeIdx  = ''
    nameIdx  = ''

    def __init__(self, code, name=""):
        self.code = code
        self.name = name

    def __str__(self):
        return f"{self.code} - {self.name}"

    def __repr__(self):
        return f"{self.code} - {self.name}{self.__dict__}"

    def __deepcopy__(self, memo):
        new_obj = Incorporator(None, 'Null')
        memo[id(self)] = new_obj
        return new_obj

    ## Formatted Code and Name print for visual checks
    def displayInfo(self,detailFlg=False):  # method without self parameter
        if detailFlg:
            print(vars(self))
        else:
            print(f"Code: {str(self.code).rjust(5, ' ')} Name: {self.name.ljust(20, ' ')}")

    ## Either convert value by convDict result or return unaltered value
    @classmethod
    def cnvattr(cls, attr):
        return lambda value: cls.convDict.get(attr, lambda value: value)(value)

    ## Rename DF column if value given in nameDict
    @classmethod
    def nameattr(cls, attr):
        return cls.nameDict.get(attr, attr)

    ## Return SubCls for data ingestion
    @classmethod
    def incSubCls(
            cls, newSubCls, codeAttr, nameAttr, endpntAPI,
            codeAdds=None, exclAdds=[], convAdds=None, nameAdds=None
    ):
        if exclAdds is None:
            exclAdds = []
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

    ## Get date and time class
    @staticmethod
    def parseDateTime(input_string, force_dt=False):
        try:
            dt = parse(input_string)
            return dt
        except (ParserError, TypeError, ValueError):
            if force_dt:
                return date.min
            else:
                return None

    ## Update Class instances from REST API source
    @classmethod
    def refreshDataREST(cls, nextUrl=None, rPath=None, nextUrlPath=None):
        ## While API pages are available loop through JSON Batches
        sessionREST = requests.Session()
        if nextUrl is None:
            nextUrl = cls.endpointAPI

        while nextUrl:
            ## Control checks for API response
            batch = cls.sessionJSON(cls.sessionAPI(sessionREST, nextUrl))

            ## Use pandas DF to normalize batch, remove exclList
            batchDF = pd.json_normalize(batch, rPath, sep="_").drop(columns=cls.exclLst)
            batchDF[cls.codeIdx] = batchDF[cls.codeIdx].apply(cls.cnvattr(cls.codeIdx))

            ## Get Code and create instances with dictionary
            batchDF = batchDF.set_index(cls.codeIdx)
            cls.createClassInstancesDICT(batchDF)

            ## Iterate DF columns to convert values
            ## Iterate DF dict of {code:row value} to update Class instances
            for col in batchDF.columns.values:
                attribDF = batchDF[col].apply(cls.cnvattr(col)).rename({col:cls.nameattr(col)}).to_dict()
                for key, value in attribDF.items():
                    setattr(cls.codeDict[key], cls.nameattr(col), value)

            nextUrl = cls.nextUrlJSON(batch, nextUrlPath)

        ## Return completed dictionary of instances
        sessionREST.close()
        return cls.codeDict