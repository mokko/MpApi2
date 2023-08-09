# MpApi - Unofficial asynchronous API client for Zetcom's MuseumPlus

API Specification: http://docs.zetcom.com/ws

## Utility / Modules
* Provides a cli utility 'monk', plus the following modules
* MpApi.aio.chunky
* MpApi.aio.client
* MpApi.aio.session

## Usage
> monk -j jobname

## DSL Format
>conf:
>	chunkSize 1111 # comment
>	exclude_modules ObjectGroup
>
>ajob:
>	# 'apack' stands for asynchronous pack
>	apack group 1234 

## Requirements
* Python 3.11 for asyncio timeout as context manager and taskGroups
* lxml
* aiohttp
* mpapi.module
* mpapi.search

For Testing
* pytest 
* pytest-asyncio

## Version History
* 20221228 - created
* 20230805 - version 0.0.2 minimal working version 

## See Also
* https://github.com/mokko/MpApi