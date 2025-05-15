# Incorporator

The Incorporator is a Super Class meant to convert data records into instances of a Subclass.

## Description

The algorithm will:
  Use PANDAS to parse JSON into DataFrames for manipulation.
  
  Create a Class dictionary for a given unique data record index/key/field
  
  Create a Class for the data records
  
  Dynamically name Class attributes for the incoming field names
  
  Convert values to primitive Python Classes or Dictionary pointers
  
  Change field/attribute field names

## Getting Started

Review Constants_API.py for SubClass parameters
Run <New Incorporator SubClass> refreshDataREST(nextURL, rPath)

### Dependencies

PANDAS for JSON Normalize, requests for JSON API call, copy for Dictionaries, dateutil for parser

### Installing

### Executing program

* How to run the program
* Step-by-step bullets
```
Review Constants_API.py for SubClass creation
Then use dictionary reference and attribute name for direct references, example: charList[4].origin
```

## Help

Only tested againg Rick and Mory API so far
```

```

## Authors


## Version History

* 0.1
    * Initial Release

## License


## Acknowledgments

Inspiration, code snippets, etc.
* [Rick And Morty API Docs](https://rickandmortyapi.com/documentation)
