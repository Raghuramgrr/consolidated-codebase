import boto3
import zipfile
import shutil
import os
from os import listdir

class FileSystem(object):
	
	@staticmethod
	def delete_file(filename):
		os.remove(filename)

	@staticmethod
	def delete_folder(folder):
		shutil.rmtree(folder)

	@staticmethod
	def list_files(folder):
		files = [f for f in listdir(folder)]
		return files
		

class S3Dataset(object):
	"""docstring for S3Dataset"""
	def __init__(self, bucket):
		super(S3Dataset, self).__init__()
		self.bucket = bucket

	def download(self, _file, output):
		s3_client = boto3.client('s3')
		s3_client.download_file(self.bucket, _file, output)

	def unzip(self, filename_from, filename_to):
		zip_ref = zipfile.ZipFile(filename_from, 'r')
		zip_ref.extractall(filename_to)
		zip_ref.close()

	def upload(self, file, key):
		s3 = boto3.resource('s3')
		data = open(file, 'rb')
		s3.Bucket(self.bucket).put_object(Key=key, Body=data)

	def list(self, folder):
		client = boto3.client('s3')
		list_objects=client.list_objects(Bucket=self.bucket, Prefix=folder, Delimiter='/')
		if "Contents" not in list_objects.keys():
			return []
		list_objects = list_objects["Contents"]
		objects = []
		for obj in list_objects:
			objects.append(obj["Key"])
		return objects

	def extract_name(self, obj_key):
		_tmp = obj_key.split("/")
		return _tmp[len(_tmp)-1]