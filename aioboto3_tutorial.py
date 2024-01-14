import boto3
import aioboto3
import os
from tqdm import tqdm
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
import shutil

session = boto3.Session()
s3_client = session.client("s3")

async_session = aioboto3.Session()

bucket = "ev-ml-data"
output_dir = "s3_exp"
os.makedirs(output_dir, exist_ok=True)
def download(bucket, key):
    s3_client.download_file(Filename="output.json", Bucket=bucket, Key=key)
key = "abhishek/datasets/obl_facet_seg_v1/val.json"
# download(bucket, key)

def get(bucket, key):
    s3_resource = session.resource("s3")
    s3_object = s3_resource.Object(bucket, key)
    content = s3_object.get()
    content = content["Body"].read()
    return content  
# get(bucket, key)

def download_folder(bucket, key):
    keys = s3_client.list_objects(Bucket=bucket, Prefix=key)
    for content in tqdm(keys["Contents"][:100]):
        outpath = os.path.join(output_dir, content["Key"].split("/")[-1])
        s3_client.download_file(Filename=outpath, Bucket=bucket, Key=content["Key"])
key = "abhishek/datasets/obl_facet_seg_v1/"
if 0:
    start = time.time()
    download_folder(bucket, key)
    print("time take to download in non-async way : ", time.time()-start)

def download_file(content):
    outpath = os.path.join(output_dir, content["Key"].split("/")[-1])
    s3_client.download_file(Filename=outpath, Bucket=bucket, Key=content["Key"])

def download_folder_with_mp(bucket, key):
    keys = s3_client.list_objects(Bucket=bucket, Prefix=key)
    # with ProcessPoolExecutor() as executor:
    with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
        executor.map(download_file, keys["Contents"])
key = "abhishek/datasets/obl_facet_seg_v1/"
if 1:
    start = time.time()
    download_folder_with_mp(bucket, key)
    print("time take to download in non-async way with mp: ", time.time()-start)

async def download_folder_async1(bucket, key):
    tasks = []
    async with async_session.client("s3") as s3_client:
        keys = await s3_client.list_objects(Bucket=bucket, Prefix=key)
        for content in tqdm(keys["Contents"]):
            outpath = os.path.join(output_dir, content["Key"].split("/")[-1])
            tasks.append(s3_client.download_file(Filename=outpath, Bucket=bucket, Key=content["Key"]))
        await asyncio.gather(*tasks)
        
key = "abhishek/datasets/obl_facet_seg_v1/"
if 1:
    start = time.time()
    asyncio.run(download_folder_async1(bucket, key))
    print("time take to download in async way1 : ", time.time()-start)

async def download_folder_async2(bucket, key):
    async with session.client("s3") as s3_client:
        keys = await s3_client.list_objects(Bucket=bucket, Prefix=key)
        for content in tqdm(keys["Contents"][:100]):
            outpath = os.path.join(output_dir, content["Key"].split("/")[-1])            
            await s3_client.download_file(Filename=outpath, Bucket=bucket, Key=content["Key"])

key = "abhishek/datasets/obl_facet_seg_v1/"
if 0:
    start = time.time()
    asyncio.run(download_folder_async2(bucket, key))
    print("time take to download in async way2 : ", time.time()-start)

async def process_chunk(keys, s3_client):
    tasks = []
    for content in tqdm(keys):
        outpath = os.path.join(output_dir, content["Key"].split("/")[-1])
        tasks.append(s3_client.download_file(Filename=outpath, Bucket=bucket, Key=content["Key"]))
    # return tasks
    await asyncio.gather(*tasks)

async def download_folder_async_with_mp(bucket, keys):
    session = aioboto3.Session()
    tasks = []
    async with session.client("s3") as s3_client:
        for content in tqdm(keys):
            outpath = os.path.join(output_dir, content["Key"].split("/")[-1])
            tasks.append(s3_client.download_file(Filename=outpath, Bucket=bucket, Key=content["Key"]))
        await asyncio.gather(*tasks)

def singleProcess(keys):
    """mission for single process"""
    loop = asyncio.new_event_loop()
    loop.run_until_complete(download_folder_async_with_mp(loop, keys))

def divide_chunks(l, n): 
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n]

async def divide_and_run(bucket, key):
    keys = s3_client.list_objects(Bucket=bucket, Prefix=key)
    chuncks = divide_chunks(keys["Contents"], 100)
    with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
        executor.map(singleProcess, chuncks)

shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)
key = "abhishek/datasets/obl_facet_seg_v1/"
if 1:
    start = time.time()
    asyncio.run(divide_and_run(bucket, key))
    print("time take to download in async way with mp: ", time.time()-start)