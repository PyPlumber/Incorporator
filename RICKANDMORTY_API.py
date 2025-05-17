from dateutil import parser
import Incorporator

## Create Super and Subs for each connected Class

RickAndMortyAPI = Incorporator.Incorporator.incSubCls('RickAndMortyAPI','id', 'name',
    'https://rickandmortyapi.com/api/',
    codeAdds = dict(),
    exclAdds = list(),
    convAdds = dict({
        'created': lambda x: parser.parse(x) if x else "",
        'location_url': lambda x: Location.codeDict.get(Location.getCodeFromUrl(x),Location.codeDict[None]),
        'origin_url': lambda x: Location.codeDict.get(Location.getCodeFromUrl(x),Location.codeDict[None])
}),
    nameAdds = dict({}))

Location = RickAndMortyAPI.incSubCls('Location','id', 'name',
    RickAndMortyAPI.endpointAPI+"location/",
    codeAdds = dict({}),
    exclAdds = list(['residents']),
    convAdds = dict({
        'residents': lambda x: list(map(lambda y: Character.codeDict.get(Location.getCodeFromUrl(y), Location.codeDict[None]), x)),
    }),
    nameAdds = dict({}))

Episode = RickAndMortyAPI.incSubCls('Episode','id', 'name',
    RickAndMortyAPI.endpointAPI+"episode/",
    codeAdds = dict({}),
    exclAdds = list([]),
    convAdds = dict({
        'air_date': lambda x: parser.parse(x) if x else "",
        'characters': lambda x: list(map(lambda y: Character.codeDict.get(Location.getCodeFromUrl(y), Location.codeDict[None]), x)),
    }),
    nameAdds = dict({}))

Character = RickAndMortyAPI.incSubCls('Character','id', 'name',
    RickAndMortyAPI.endpointAPI+"character/",
    codeAdds = dict({}),
    exclAdds = list([
        'episode',
        'origin_name',
        'location_name',
        'image'
    ]),
    convAdds = dict({
        'episode': lambda x: list(map(lambda y: Episode.codeDict.get(Episode.getCodeFromUrl(y),Episode.codeDict[None]), x))
    }),
    nameAdds = dict({'origin_url':'origin'}))
