## How to set the environment for this challenge:

### Requirements:
- Docker
- Docker-Compose

### Steps

1. Install docker and docker compose on your machine, you can follow this tutorial to help you out:

https://support.netfoundry.io/hc/en-us/articles/360057865692-Installing-Docker-and-docker-compose-for-Ubuntu-20-04

2. Inside this repository, you will find a docker-compose.yml file. Run, in the root of the repo, the command:
`docker-compose up` to start a instance of the postgres database. If you see the message, `postgres_1  | PostgreSQL init process complete; ready for start up.` in your terminal the initialization went well.
 This step above will initialize a postgres database with sample data already on it. You will need to read the data from it and the information you need to connect are:


```
host=localhost
port=5432
user=postgres
password=postgres
```
You can check if everything is ok by connecting to the database using Dbeaver, Datagrip, etc.

3. To stop the docker container, from the root of the repo, use the command `docker-compose down`