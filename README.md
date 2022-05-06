# Resume
This is a simple class made for personal use that allows get messages and users from a telegram group. Principally i used Telethon, Boto3, Pandas and PyArrow.  The telegram credentials are getted from AWS Secrets Manager `telegram/credentials` and the results are stored in s3 buckets for default.

# How-To
1. Import file from Github and asyncio <br />
`import git@github.com:dalbeh/telegram-telethon.git as telegramTelethon` <br />
`import asyncio`

2. Create class instance
`telegram = telegramTelethon.telegramBot()`

3. Create async loop
`loop = asyncio.get_event_loop()`

5. Call connect() method and get the client object
`client = loop.run_until_complete(telegram.connect())`

7. Call getMessages() or getParticipants() methods
`loop.run_until_complete(telegram.getParticipants(client, group, limit, type))
loop.run_until_complete(telegram.getMessages(client, group, limit, daily))`
