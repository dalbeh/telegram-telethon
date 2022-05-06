# Resume
This is a simple class maded for personal use that allows get messages and users from a telegram group. Principally i used Telethon, Boto3, Pandas and PyArrow.  The telegram credentials are getted from AWS Secrets Manager `telegram/credentials` and the results are stored in s3 buckets for default.
<br />
# How-To
1. Import file from Github and asyncio <br />
`import git@github.com:dalbeh/telegram-telethon.git as telegramTelethon` <br />
`import asyncio`
<br />
2. Create class instance <br />
`telegram = telegramTelethon.telegramBot()`
<br />
3. Create async loop <br />
`loop = asyncio.get_event_loop()`
<br />
5. Call connect() method and get the client object <br />
`client = loop.run_until_complete(telegram.connect())`
<br />
7. Call getMessages() or getParticipants() methods <br />
`loop.run_until_complete(telegram.getParticipants(client, group, limit, type))` <br />
`loop.run_until_complete(telegram.getMessages(client, group, limit, daily))`
