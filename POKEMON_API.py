import Incorporator

PokemonBase = Incorporator.Incorporator.incSubCls('PokemonBase','id', 'name',
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


## :"https://pokeapi.co/api/v2/ability/?limit=20&offset=20"
