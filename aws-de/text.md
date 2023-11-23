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

echo "retail_db/" >> .gitignore to add to the retail db to git ignore