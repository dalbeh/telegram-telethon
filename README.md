# Resume
This is a simple class maded for personal use that allows to get messages and users from a telegram group. Principally i used Telethon, Boto3 and Pandas.  The telegram credentials are getted from AWS Secrets Manager `telegram/credentials` and the results are stored in s3 buckets for default.
<br />
## How-To: Technical Use
1. On AWS, go to Secrets Manager and create a secret named `telegram/credentials` with the following structure filling it with your data <br />
```
{
    "api_id": "",
    "api_hash": "",
    "phone": "",
    "username": ""
}
```

2. Import file from Github using httpimport and asyncio <br />
```
import httpimport
with httpimport.github_repo('dalbeh', 'telegram-telethon', 'telegramTelethon', branch='main'):
    import telegramTelethon
import asyncio
```

2. Create class instance <br />
`telegram = telegramTelethon.telegramBot(s3bucket)`
3. Create async loop <br />
`loop = asyncio.get_event_loop()`

5. Call connect() method and get the client object <br />
`client = loop.run_until_complete(telegram.connect())`

7. Call getMessages(), getMessagesFromDays() or getParticipants() methods <br />
`loop.run_until_complete(telegram.getMessages(client, group, limit, daily))` <br />
`loop.run_until_complete(telegramClient.getMessagesFromDays(client, group, limit, date_from, date_to))` <br />
`loop.run_until_complete(telegram.getParticipants(client, group, limit, type))`

