import incorporator as Incorp

## Create Super and Subs for each connected Class

RickAndMortyAPI = Incorp.Incorporator.incorp('RickAndMortyAPI', 'id', 'name',
    url='https://rickandmortyapi.com/api/',rPath='results',
    codeAdds = dict(),
    exclAdds = list(),
    convAdds = dict({
        'created': lambda x: RickAndMortyAPI.parseDateTime(x),
        'location_url': lambda x: Location.codeDict.get(Location.getCodeFromUrlAPI(x), ""),
        'origin_url': lambda x: Location.codeDict.get(Location.getCodeFromUrlAPI(x), "")
    }),
    nameAdds = dict({}))

Location = RickAndMortyAPI.incorp('Location', 'id', 'name',
    url=RickAndMortyAPI.url +"location/",rPath='results',
    codeAdds = dict({}),
    exclAdds = list(['residents']),
    convAdds = dict({
        'residents': lambda x: list(map(lambda y: Character.codeDict.get(Location.getCodeFromUrlAPI(y), ""), x)),
    }),
    nameAdds = dict({}))

Episode = RickAndMortyAPI.incorp('Episode', 'id', 'name',
    url=RickAndMortyAPI.url +"episode/",
    codeAdds = dict({}),
    exclAdds = list([]),
    convAdds = dict({
        'air_date': lambda x: RickAndMortyAPI.parseDateTime(x),
        'characters': lambda x: list(map(lambda y: Character.codeDict.get(Location.getCodeFromUrlAPI(y), ""), x)),
    }),
    nameAdds = dict({}))

Character = RickAndMortyAPI.incorp('Character', 'id', 'name',
    url=RickAndMortyAPI.url +"character/",
    codeAdds = dict({}),
    exclAdds = list([
        'episode',
        'origin_name',
        'location_name',
        'image'
    ]),
    convAdds = dict({
        'episode': lambda x: list(map(lambda y: Episode.codeDict.get(Episode.getCodeFromUrlAPI(y), Episode.codeDict[None]), x))
    }),
    nameAdds = dict({'origin_url':'origin'}))