import pandas as pd

class IncorpDictMixin:
## Return known instance or create new one at dictionary key
    @classmethod
    def getOrCreateDICT(cls, code, name):
        if code not in cls.codeDict:
            cls.codeDict.update({code: cls(code, name)})
        return cls.codeDict[code]

    ## Create Class Instances and  dictionary entires
    @classmethod
    def createClassInstancesDICT(cls, createDF):
        ## set Class code as index,
        createDict = createDF[cls.nameIdx].to_dict()

        ## Iterate Batch dict to find OR create
        for key, value in createDict.items():
            cls.getOrCreateDICT(key, value)
