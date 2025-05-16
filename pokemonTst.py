import POKEMON_API

langList = POKEMON_API.Language.refreshDataREST(POKEMON_API.Language.endpointAPI, rPath='results', nextUrlPath=['next'])
print(langList[1])

pokeList = POKEMON_API.PokeSpecies.refreshDataREST(POKEMON_API.PokeSpecies.endpointAPI, rPath='results', nextUrlPath=['next'])

print(len(pokeList.values()))
print(pokeList[3].name)
print(pokeList[121].name)

pokeList[125].displayInfo()

