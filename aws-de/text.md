Set up the local dev enviornment on mac

create an iam user for myself from my root acct

set up the aws cli with my iam user credentials

set up my venv on mac

python3 -m venv myenv

source de-venv/bin/activate

add Boto3

set up jupyter lab and validad boto3

### See File ###
Getting Started.ipynb


### Set up Cloud9 ###
Create IAM Group ITVCloud9 and attach the Admin policy to it

Create an IAM user ITVCloud9User and add it the to Group ITVCloud9

Login as IAM User ITVCloud9User

Go to the Cloud9 Console

Create a Cloud9 instance using a Linux 2

Leverage this to explore Amazon related services 

### Cloud9 Tests###

Test Python by creating a "Hello World" File

Test Git using "git"

Test docker using "docker ps"

Test "docker ps -a"

Test "sudo systemctl status docker"

Test sudo systemctl status httpd

Test "python"

Test "python3"

Test "java -version"

Test "javac -version"

### Get the retail database ###

in the terminal 

cd path / to / portfolio

git clone --depth 1 https://github.com/dgadiraju/retail_db.git

mv retail_db/* retail_db/.git* retail_db/

rm -rf retail_db/.git

Here's what each command does:

git clone --depth 1 clones the repository with just the latest commit.

mv moves the contents to your "retail_db" folder.

rm -rf retail_db/.git removes the .git directory, effectively making "retail_db" a regular directory without Git tracking.

if you want to git gignore the db -- > echo "retail_db/" >> .gitignore to add to the retail db to git ignore

### Create s3 Bucket ###

Create a role called AWSS3FullAccess and assign the S3 full access policy

Create s3 bucket

create a replication bucket for cross region fault tolerance using the role AWSS3FullAccess

limit the scope to one filter, prefix retail_db

Go back and change the storage class of the replication file to glacier deep archive

Add lifecycle rules to transfew noncuncurrent versions to Glacier deep archive, then delete peremanantly

practice using the cli to copy local files to a bucket

    create a bucket, copy the files, delete the files, remove the bucket

Create a user using the cli and give the user read only access to s3

aws iam create-user --user-name 'itvsupport1'

aws iam attach-user-policy --user-name 'itvsupport1' --policy-arn 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'


