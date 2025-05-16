import pandas as pd
import RICKANDMORTY_API

# print(vars(RICKANDMORTY_API.RickAndMortyAPI))
# print(vars(RICKANDMORTY_API.Location))
# print(vars(RICKANDMORTY_API.Episode))
# print(vars(RICKANDMORTY_API.Character))

locList = RICKANDMORTY_API.Location.refreshDataREST(RICKANDMORTY_API.Location.endpointAPI, rPath='results', nextUrlPath=['info','next'])
print(locList[23])
print(locList[16].name)
print("\n")

charList = RICKANDMORTY_API.Character.refreshDataREST(RICKANDMORTY_API.Character.endpointAPI, rPath='results', nextUrlPath=['info','next'])
print(charList[4])
print(charList[4].gender)
print(charList[4].origin)
charList[4].origin.displayInfo()
charList[33].displayInfo(True)

print("\n")

epsList = RICKANDMORTY_API.Episode.refreshDataREST(RICKANDMORTY_API.Episode.endpointAPI, rPath='results', nextUrlPath=['info','next'])
print(epsList[4])

print("\n")

story = epsList[7]
print(f"Episode {story.code} was titled {story.name}.")
print(f"The episode aired on {story.air_date.strftime('%d, %b %Y')}.")
print(f"The API instance was created in {story.created.strftime('%Y')}.")

print("\n")

cast = story.characters
print(f"It had {len(cast)} characters:")
for character in cast:
    character.displayInfo()


print("\n")

origins = []
for character in cast:
    origins.append([character.name, character.origin.name])

originsDF = pd.DataFrame(origins, columns =['Character', 'Origin'])
print(f"Here is episode {story.code}'s character origin distribution:")
print(originsDF.value_counts('Origin'))
