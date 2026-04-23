import incorporator as Incorp

PokemonBase = Incorp.Incorporator.incorp('PokemonBase', 'id', 'name',
    url='https://pokeapi.co/api/v2/',rPath='results',
    codeAdds= {},
    exclAdds=[],
    convAdds={
        'url': lambda x: PokemonBase.getCodeFromUrlAPI(x)
    },
    nameAdds={})


Language = PokemonBase.incorp('Language', 'url', 'name',
    url=PokemonBase.url +'language/',rPath='results',
    codeAdds= {},
    exclAdds=[],
    convAdds={
    },
    nameAdds={})

PokeSpecies = PokemonBase.incorp('PokeSpecies', 'url', 'name',
    url=PokemonBase.url +'pokemon-species/',rPath='results',
    codeAdds= {},
    exclAdds=[],
    convAdds={
    },
    nameAdds={})