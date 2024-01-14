from async_s3 import AsyncS3
from s3 import S3
import os
import time

s3_obj = S3()
s3_client = s3_obj.session.client("s3")
asyncs3_obj = AsyncS3()

if __name__ == "__main__":
    bucket = "ev-ml-data"
    output_dir = "s3_exp"
    os.makedirs(output_dir, exist_ok=True)

    key = "abhishek/datasets/obl_facet_seg_v1/"
    start = time.time()
    s3_obj.download_many(s3_client, bucket, key, output_dir)
    print("Time Taken to download using non-async multiprocessing ", time.time()-start)

    start = time.time()
    asyncs3_obj.download_many(bucket, key, output_dir)
    print("Time Taken to download using non-async multiprocessing ", time.time()-start)