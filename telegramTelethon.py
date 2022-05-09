# This class is used to connect to the Telegram API and download messages and users from a group
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import PeerChannel
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.types import ChannelParticipantsAdmins
from telethon.tl.types import ChannelParticipantBanned
from telethon.tl.types import ChannelParticipantsBots
from telethon.tl.types import ChannelParticipantsRecent
from telethon.tl.types import ChannelParticipantsMentions
import pyarrow.parquet as pq
import pandas as pd
import boto3
import s3fs
import datetime
import time
import json


class telegramBot:
    
    def __init__(self, s3bucket):
        """
        It gets the credentials from the getCredentials() method and assigns them to the variables
        username, api_id, api_hash, and phone
    
        :param s3bucket: The name of the S3 bucket where the results will be stored
        """
        self.username, self.api_id, self.api_hash, self.phone = self.getCredentials()
        self.today_date = time.strftime('%d/%m/%y', time.localtime())
        self.s3bucket = s3bucket


    def getCredentials(self):
        """
        It uses the boto3 library to connect to AWS Secrets Manager and retrieve the secret value of the
        secret named 'telegram/credentials'
        :return: username, api_id, api_hash, phone
        """

        # import boto3
        secrets_manager = boto3.client('secretsmanager')
        telegram_credentials = secrets_manager.get_secret_value(SecretId='telegram/credentials') 
        telegram_credentials = json.loads(telegram_credentials['SecretString'])


        return telegram_credentials['username'],telegram_credentials['api_id'],telegram_credentials['api_hash'],telegram_credentials['phone']


    async def connect(self):
        """
        It creates a new TelegramClient object, and then starts it with the phone number and api_id and
        api_hash
        :return: The client object is being returned.
        """

        client = await TelegramClient(self.username, self.api_id, self.api_hash).start(phone=self.phone)

        # Ensure you're authorized
        if await client.is_user_authorized() == False:
            await client.send_code_request(self.phone)
            try:
                await client.sign_in(self.phone, input('Enter the code: '))
            except SessionPasswordNeededError:
                await client.sign_in(password=input('Password: '))

        return client


    async def getMessages(self,client,group:str,total_messages:int, daily:bool):
        """
        It downloads the messages from a group, filters them by date and by bots, and returns a
        dataframe with the messages
        
        :param client: The client object that you created in the previous step
        :param group: The group ID or username
        :param total_messages: The total number of messages in the group
        :param daily: True if you want to download only the messages from today, False if you want to
        download all the messages from the group
        :return: A dataframe with the messages, user_id, date_message and group
        """

        if group.isdigit():
            entity = PeerChannel(int(group))
        else:
            entity = group


        my_channel = await client.get_entity(entity)

        bots_id = self.getBotList()        
    
        today_date_start = self.today_date +' 00:00:00'
        today_date_start = datetime.datetime.strptime(today_date_start, '%d/%m/%y %H:%M:%S')
        today_date_start_unix = datetime.datetime.timestamp(today_date_start)*1000

        today_date_end = self.today_date +' 23:59:59' 
        today_date_end = datetime.datetime.strptime(today_date_end, '%d/%m/%y %H:%M:%S')

        print('Downloading Messages ' + group)
        df = pd.DataFrame(columns=['message', 'user_id', 'date_message', 'group'])
        gettingOtherDay = False
        offset = 0
        i = 1
        download = True
        while download == True:

            history = await client(GetHistoryRequest(
                    peer=my_channel,
                    offset_id=0,
                    offset_date=today_date_end,              
                    add_offset=offset,
                    limit=500,
                    max_id=0,
                    min_id=0,
                    hash=0
                ))

            messages = history.messages
            for message in messages:
                try:
                    message_dict=message.to_dict()
                    
                
                    from_id = message_dict['from_id']
                    user_id = str(from_id['user_id'])
                    date_message = message_dict['date']
                    date_message_unix = datetime.datetime.timestamp(message_dict['date'])*1000

        
                    if (date_message_unix >= today_date_start_unix or daily == False) and int(user_id) not in bots_id and len(str(message_dict['message'])) > 2 and str(message_dict['message'])[:1] != '/':
                        df_lenght = len(df) 
                        df.loc[df_lenght]=[message_dict['message'], user_id, date_message, group]
                    elif daily and date_message_unix >= today_date_start_unix:
                        gettingOtherDay = True
                    
                    
                except: 
                    pass
            
            offset = offset + 500
            print(str(round(((offset/total_messages))*100,2))+'%', end= '\r')
            if offset >= total_messages or gettingOtherDay:
                download = False

        group_name = group.split('/')[3].lower()
        if daily == True: 
            date_file = str(self.today_date).replace('/', '-')
        else: 
            date_file = 'historic'
        name = f'messages_{date_file}_{group_name}'

        return self.toParquetAndS3(df,name)


    async def getMessagesFromDays(self,client,group:str,total_messages:int,date_from:str,date_to:str):
        """
        It downloads the messages from a group in a given date range, filters them by date and by bots, and returns a
        dataframe with the messages

        :param client: The client object that you created in the previous step
        :param group: The group ID or username
        :type group: str
        :param total_messages: The total number of messages per date.
        :type total_messages: int
        :param date_from: The date from which you want to start downloading messages
        :type date_from: str
        :param date_to: The date to downloading messages from
        :type date_to: str
        :return: A list of messages from a group
        """
        
        
        # date_from, date_to example: 01/12/22 > DD/MM/YY
            
        date_from_start = date_from +' 00:00:00' 
        date_from_start = datetime.datetime.strptime(date_from_start, '%d/%m/%y %H:%M:%S')
        date_to_start = date_to +' 00:00:00' 
        date_to_start = datetime.datetime.strptime(date_to_start, '%d/%m/%y %H:%M:%S')

        days = (date_to_start - date_from_start).days+1

        datelist = pd.date_range(date_from_start, periods=days).tolist()
        

        if group.isdigit():
            entity = PeerChannel(int(group))
        else:
            entity = group


        my_channel = await client.get_entity(entity)

        bots_id = self.getBotList()        


        print('Downloading Messages ' + group)
        df = pd.DataFrame(columns=['message', 'user_id', 'date_message', 'group'])

        for _date in datelist:
        
            date_start = _date
            # print(datetime.datetime.utctimetuple((date_start)))
            date_start_unix = datetime.datetime.timestamp(date_start)*1000
            date_start_text = time.strftime('%d/%m/%y', datetime.datetime.utctimetuple((date_start)) ).replace('/', '-')


            date_end_offset = _date + datetime.timedelta(hours=23.999999999)
            date_end_offset_unix = datetime.datetime.timestamp(date_end_offset)*1000


            offset = 0
            i = 1
            download = True
            while download == True:

                history = await client(GetHistoryRequest(
                        peer=my_channel,
                        offset_id=0,
                        offset_date=date_end_offset,              
                        add_offset=offset,
                        limit=500,
                        max_id=0,
                        min_id=0,
                        hash=0
                    ))

                messages = history.messages
                for message in messages:
                    try:
                        message_dict=message.to_dict()
                        
                    
                        from_id = message_dict['from_id']
                        user_id = str(from_id['user_id'])
                        date_message = message_dict['date']
                        date_message_unix = datetime.datetime.timestamp(message_dict['date'])*1000

            
                        if (date_message_unix >= date_start_unix) and int(user_id) not in bots_id and len(str(message_dict['message'])) > 2 and str(message_dict['message'])[:1] != '/':
                            df_lenght = len(df) 
                            df.loc[df_lenght]=[message_dict['message'], user_id, date_message, group]
                        elif date_message_unix < date_start_unix:
                            gettingOtherDay = True
                        
                        
                    except: 
                        pass
                
                offset = offset + 500
                print(str(round(((offset/total_messages))*100,2))+'%', end= '\r')
                if offset >= total_messages or gettingOtherDay:
                    download = False


            group_name = group.split('/')[3].lower()
            name = f'messages_{date_start_text}_{group_name}'
            self.toParquetAndS3(df,name)
            # self.toCSVAndS3(df,name)

        return print('Done')


    async def getParticipants(self,client,group,total_users:int,type:str):
        """
        It downloads all the users from a group, and returns a dataframe with the following columns: id,
        first_name, last_name, username, phone, group
        
        :param client: The client you're using to connect to Telegram
        :param group: The group ID or username
        :param total_users: The total number of users in the group
        :param type: The type of users you want to download
        :return: A dataframe with the following columns:
        """


        if group.isdigit():
            entity = PeerChannel(int(group))
        else:
            entity = group


        my_channel =  await client.get_entity(entity)


        print('Downloading Users ' + type.capitalize() + ' From ' + group)

        df = pd.DataFrame(columns=['id', 'first_name', 'last_name', 'username', 'phone', 'group']) 
    
        

        if type.upper() == 'ALL':

            # Search participants by letter in name
            # Without the filter, the query only returns 10k members max
            # Workaround based on https://github.com/LonamiWebs/Telethon/issues/580 

            queryKey = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
        
            for letter in queryKey:

                print('Letter: ' + letter.upper())
                offset = 0
                i = 1
                download = True
                while download == True:

                    result = await client(GetParticipantsRequest(channel=my_channel,  offset= offset, filter = ChannelParticipantsSearch(letter), limit=200, hash=0))

                    for user in result.users:

                        df_lenght = len(df) 
                        df.loc[df_lenght]=[user.id, user.first_name, user.last_name, user.username, user.phone, group]

                    offset = offset + 100
                    print(str(round(((offset/total_users))*100,2))+'%', end= '\r')
                    if offset >= total_users:
                        download = False

        else:

            if type.upper() == 'ADMIN':
                filter = ChannelParticipantsAdmins()

            elif type.upper() == 'BANNED':
                filter = ChannelParticipantBanned()

            elif type.upper() == 'BOT':
                filter = ChannelParticipantsBots()

            elif type.upper() == 'RECENT':
                filter = ChannelParticipantsRecent()

            elif type.upper() == 'MENTIONS':
                filter = ChannelParticipantsMentions()


            offset = 0
            i = 1
            download = True
            while download == True:

                result = await client(GetParticipantsRequest(channel=my_channel,  offset= offset, filter = filter, limit=200, hash=0))

                for user in result.users:

                    df_lenght = len(df) 
                    df.loc[df_lenght]=[user.id, user.first_name, user.last_name, user.username, user.phone, group]

                offset = offset + 100
                print(str(round(((offset/total_users))*100,2))+'%', end= '\r')
                if offset >= total_users:
                    download = False
        
        group_name = group.split('/')[3].lower()
        date_file = str(self.today_date).replace('/', '-')
        name = f'users_{str(type).lower()}_{date_file}_{group_name}'
        return self.toParquetAndS3(df,name)


    def toParquetAndS3(self,df,name):
        """
        It takes a dataframe, converts the dataframe to a parquet, and uploads it to an s3 bucket
        
        :param df: the dataframe to be uploaded
        :param name: The name of the file
        :return: print statement with s3 path.
        """
        s3_url = f's3://{self.s3bucket}/telegram/{name}.parquet.gzip'
        df.to_parquet(s3_url, compression='gzip')
        return print('Uploaded to ' + s3_url)


    def toCSVAndS3(self,df,name):
        """
        It takes a dataframe, converts the dataframe to a csv, and uploads it to an s3 bucket
        
        :param df: the dataframe you want to upload
        :param name: The name of the file
        :return: print statement with s3 path.
        """
        s3_url = f's3://{self.s3bucket}/telegram/{name}.csv'
        df.to_csv(s3_url, index=False)
        return print('Uploaded to ' + s3_url)


    def getBotList(self):
        """
        It tries to get the bot list from today's date, if it fails, it tries to get the bot list from
        yesterday's date
        :return: A list of bot user ids
        """
        try:
            print("Trying to get today's bot id list")
            today_date = time.strftime('%d/%m/%y', (datetime.date.today()).timetuple()).replace('/', '-')
            s3 = s3fs.S3FileSystem()
            df_bots = pq.ParquetDataset(f's3://raw-data-extracted/telegram/users_bot_{today_date}_walkwithsteptelegram.parquet.gzip', filesystem=s3).read_pandas().to_pandas()
            return list(df_bots['id'])
        except:
            print("Trying to get yesterday's bot id list")
            yesterday_date = time.strftime('%d/%m/%y', (datetime.date.today() - datetime.timedelta(1)).timetuple()).replace('/', '-')
            s3 = s3fs.S3FileSystem()
            df_bots = pq.ParquetDataset(f's3://raw-data-extracted/telegram/users_bot_{yesterday_date}_walkwithsteptelegram.parquet.gzip', filesystem=s3).read_pandas().to_pandas()
            return list(df_bots['id'])
