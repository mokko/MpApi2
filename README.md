# MpApi - Unofficial asynchronous API client for Zetcom's MuseumPlus

API Specification: http://docs.zetcom.com/ws

## Utility / Modules
* Provides a cli utility 'monk', plus the following modules
* MpApi.aio.chunky
* MpApi.aio.client (only a few endpoints implemented at the moment)
* MpApi.aio.session

## Usage
> monk -j jobname

## DSL Format
```
conf:
	chunkSize 800 # comment
	exclude_modules ObjectGroup
	chunks 2 # parallel chunks
	semaphore 10 
ajob:
	# 'apack' stands for asynchronous pack
	apack group 1234 # possible query types: approval, exhibit, group, loc, query
	
```

## Requirements
* Python probably 3.9 for asyncio (currently we dont use TaskGroups) 
* lxml
* aiohttp
* MpApi (https://github.com/mokko/MpApi)

For Testing
* pytest 
* pytest-asyncio

## Version History
* 20221228 - created
* 20230811 - version 0.0.2 minimal working version with parallel chunks
