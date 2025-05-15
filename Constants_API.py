from dateutil import parser
import Incorporator

## Create Super and Subs for each connected Class

RickAndMortyAPI = Incorporator.Incorporator.incSubCls('RickAndMortyAPI','id', 'name',
    'https://rickandmortyapi.com/api/',
    codeAdds= {},
    exclAdds=['url'],
    convAdds={
        'residents': lambda x: list(map(lambda y: Character.codeDict.get(Location.getCodeFromUrl(y),Location.codeDict[None]), x)),
        'created': lambda x: parser.parse(x) if x else "",
        'air_date': lambda x: parser.parse(x) if x else "",
        'characters': lambda x: list(map(lambda y: Character.codeDict.get(Location.getCodeFromUrl(y),Location.codeDict[None]), x)),
        'location_url': lambda x: Location.codeDict.get(Location.getCodeFromUrl(x),Location.codeDict[None]),
        'origin_url': lambda x: Location.codeDict.get(Location.getCodeFromUrl(x),Location.codeDict[None])
    },
    nameAdds={})

Location = RickAndMortyAPI.incSubCls('Location','id', 'name',
    RickAndMortyAPI.endpointAPI+"location/",
    codeAdds= {},
    exclAdds=['residents'],
    convAdds={},
    nameAdds={})

Episode = RickAndMortyAPI.incSubCls('Episode','id', 'name',
    RickAndMortyAPI.endpointAPI+"episode/",
    codeAdds= {},
    exclAdds=[],
    convAdds={},
    nameAdds={})

Character = RickAndMortyAPI.incSubCls('Character','id', 'name',
    RickAndMortyAPI.endpointAPI+"character/",
    codeAdds= {},
    exclAdds=[
        'episode',
        'origin_name',
        'location_name',
        'image'
    ],
    convAdds={
        'episode': lambda x: list(map(lambda y: Episode.codeDict.get(Episode.getCodeFromUrl(y),Episode.codeDict[None]), x))
    },
    nameAdds={'origin_url':'origin'})
