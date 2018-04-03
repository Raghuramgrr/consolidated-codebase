import boto3
import zipfile
import csv
import os
from os import listdir
import shutil

BUCKET_NAME='daimler-phase1'
UNZIPPED_FILE_DESTINATION="unzipped_clickstream"
TEMPORARY_ZIP="tmp.zip"
TEMPORARY_FOLDER="tmp.tsv"
CSV_FILE = "tmp.csv"

def list_files_to_unzip():
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(BUCKET_NAME)
	already_unzipped = {}
	list_objects=[]
	for obj in bucket.objects.all():
		if obj.key.split("/")[0] == UNZIPPED_FILE_DESTINATION:
			already_unzipped[obj.key] = True
		else:
			typefile = obj.key.split(".")
			typefile = typefile[len(typefile)-1]
			if typefile == "zip" and "dailic00162" in obj.key:
				list_objects.append(obj.key)
	to_unzip = []
	for _file in list_objects:
		_tmp = _file.split("/")
		_tmp = UNZIPPED_FILE_DESTINATION+"/"+"/".join(_tmp[1:len(_tmp)])
		if _tmp not in already_unzipped.keys():
			to_unzip.append(_file)
	return to_unzip

def download(key):
	s3_client = boto3.client('s3')
	s3_client.download_file(BUCKET_NAME, key, TEMPORARY_ZIP)

def unzip(filename_from, filename_to):
	zip_ref = zipfile.ZipFile(filename_from, 'r')
	zip_ref.extractall(filename_to)
	zip_ref.close()

def convert_to_csv(tsv_file, csv_file):
	# read tab-delimited file
	with open(tsv_file,'rb') as fin:
		cr = csv.reader(fin, delimiter='\t')
		filecontents = [line for line in cr]
	# write comma-delimited file (comma is the default delimiter)
	with open(csv_file,'wb') as fou:
		cw = csv.writer(fou, quotechar='', quoting=csv.QUOTE_NONE, escapechar='\\')
		cw.writerows(filecontents)

def delete_file(filename):
	os.remove(filename)

def delete_folder(folder):
	shutil.rmtree(folder)

def get_file_from_folder(folder):
	files = [f for f in listdir(folder)]
	return files[0]

def csv_filename(tsv_file):
	names = tsv_file.split(".")
	return ".".join(names[:len(names)-1])+".csv"

def upload_file(file, key):
	s3 = boto3.resource('s3')
	data = open(file, 'rb')
	s3.Bucket(BUCKET_NAME).put_object(Key=UNZIPPED_FILE_DESTINATION+"/"+key, Body=data)



print("LISTING S3 FILES")
files = list_files_to_unzip()
download(files[0])
unzip(TEMPORARY_ZIP, TEMPORARY_FOLDER)
delete_file(TEMPORARY_ZIP)
print("CONVERTING TO CSV")
tsv_filename = get_file_from_folder(TEMPORARY_FOLDER)
# csv_filename = csv_filename(tsv_filename)
# convert_to_csv(TEMPORARY_FOLDER+"/"+tsv_filename, csv_filename)
# delete_folder(TEMPORARY_FOLDER)
# upload_file(csv_filename)
# delete_file(csv_filename)

