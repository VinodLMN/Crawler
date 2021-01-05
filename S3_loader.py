
import boto3
from io import StringIO

class S3_loader(object):

    def __init__(self):
        self.s3client = boto3.client('s3', aws_access_key_id='key', aws_secret_access_key='secret_key')

    def copy_to_s3(self, df, bucket, filepath):
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        self.s3client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
        print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')
