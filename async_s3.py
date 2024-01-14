import aioboto3
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import os
import tqdm as tqdm

def divide_chunks(l, n): 
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n]

class AsyncS3():
    def __init__(self):
        self.session = aioboto3.Session()
    def get_object(self):
        pass
    def download(self):
        pass
    def upload(self):
        pass

    def call_async(self, keys):
        """mission for single process"""
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.download_keys(loop, keys))

    async def download_keys(s3_bucket, s3_keys, output_dir):
        session = aioboto3.Session()
        tasks = []
        async with session.client("s3") as s3_client:
            for content in tqdm(s3_keys):
                outpath = os.path.join(output_dir, content["Key"].split("/")[-1])
                tasks.append(s3_client.download_file(Filename=outpath, Bucket=s3_bucket, Key=content["Key"]))
            await asyncio.gather(*tasks)
    
    def download_many(self, s3_client, s3_bucket, s3_key):
        keys = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_key)
        chuncks = divide_chunks(keys["Contents"], 100)
        with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
            executor.map(self.call_async, chuncks)

    def upload_many(self):
        pass
