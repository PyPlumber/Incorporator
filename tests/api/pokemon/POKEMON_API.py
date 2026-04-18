import Incorporator as Incorp

PokemonBase = Incorp.Incorporator.incSubCls('PokemonBase', 'id', 'name',
    'https://pokeapi.co/api/v2/',
    codeAdds= {},
    exclAdds=[],
    convAdds={
        'url': lambda x: PokemonBase.getCodeFromUrl(x)
    },
    nameAdds={})

Language = PokemonBase.incSubCls('Language','url', 'name',
    PokemonBase.endpointAPI+'language/',
    codeAdds= {},
    exclAdds=[],
    convAdds={
    },
    nameAdds={})

PokeSpecies = PokemonBase.incSubCls('PokeSpecies','url', 'name',
    PokemonBase.endpointAPI+'pokemon-species/',
    codeAdds= {},
    exclAdds=[],
    convAdds={
    },
    nameAdds={})

