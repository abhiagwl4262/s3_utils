import boto3
from concurrent.futures import ThreadPoolExecutor
import glob
import os
from tqdm import tqdm

def divide_chunks(l, n): 
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n]

class S3:
    def __init__(self):
        self.session = boto3.Session()
        self.s3_client = self.session.client("s3")

    def getS3Obj(self, s3_client, s3_bucket, s3_key):
        """
        send request and retrieve the obj from S3
        """
        resp = s3_client.get_object(
            Bucket=s3_bucket,
            Key=s3_key
        )
        obj = resp['Body'].read()
        return obj

    def download(self, s3_client, s3_bucket, s3_key, outpath):
        s3_client.download_file(Filename=outpath, Bucket=s3_bucket, Key=s3_key)

    def upload(self, s3_client, s3_bucket, s3_key, outpath):
        s3_client.upload_file(Filename=outpath, Bucket=s3_bucket, Key=s3_key)

    def download_many(self, s3_client, s3_bucket, s3_key, output_dir):
        keys = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_key)
        with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
            executor.map(download_file, keys["Contents"])

    def upload_many(self, s3_client, s3_bucket, s3_key, input_dir):
        fpaths = glob.glob(input_dir+"/*")
        for fpath in tqdm(fpaths):
            s3_client.upload_file(Filename=fpath, Bucket=s3_bucket, Key=fpath.split("/")[-1])
