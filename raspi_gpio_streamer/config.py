import os

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "")
HW_ID = os.uname().nodename
