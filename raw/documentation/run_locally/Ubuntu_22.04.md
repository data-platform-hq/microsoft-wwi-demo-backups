# “raw-microsoft-wwi” local run on Ubuntu 22.04

### Prerequisitions
- [Docker](https://www.docker.com/products/docker-desktop/) Desktop
- Java (JDK) is required. Please set up JDK 11 version.
```shell
$ sudo apt-get install openjdk-11-jdk
```

### 1 Download the “raw-microsoft-wwi” project
Clone the “raw-microsoft-wwi” project from [EPAM repository](https://git.epam.com/epma-dpaf/iacda/data-pipelines/databricks-raw-microsoft-wwi) to your ”/home/<user_name>” folder and check it

### 2 Fill all variables in “.env.dist” file and then copy it with renaming into “.env”
Please change "DELTA_TABLES_DEST" and "TEST_TABLES_DEST" variables for delta tables and "EVENT_HUBS_NAMESPACE", "EVENT_HUBS_ACCESS_KEY_NAME", "EVENT_HUBS_ACCESS_KEY" if you want to stream data from Azure Eventhubs. Other variables changing is optional, defaults can be used
```shell
DELTA_TABLES_DEST=/home/<user_name>/raw-microsoft-wwi/delta-tables/destination
TEST_TABLES_DEST=/home/<user_name>/raw-microsoft-wwi/test_csv
```  
![linux_variables.png](img/linux_variables.png)
### 3 Install virtual environment
#### 3.1 Install homebrew Package Manager https://brew.sh/
```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
Run these three commands in your terminal to add Homebrew to your PATH changing your user_name:
```shell
$ echo '# Set PATH, MANPATH, etc., for Homebrew.' >> /home/<user_name>/.profile
$ echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> /home/<user_name>/.profile
$ eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
```

Install Homebrew's dependencies:
```shell
$ sudo apt-get install build-essential
```

#### 3.2 Install pyenv
```shell
$ brew install pyenv
```

### 4 Install [poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
```shell
$ curl -sSL https://install.python-poetry.org | python3 -
```

### 5 Check the “pyproject.toml” file and installs dependencies using
```shell
$ ./install.sh 3.8.13
```

If you have "ModuleNotFoundError: No module named 'distutils.cmd'" - install distutils and run "install.sh" again
```shell
$ sudo apt-get install python3.8-distutils
```

### 6 Copy the microsoft jdbc driver jars into Spark/jars
Copy the [mssql-jdbc-8.4.0.jre11](https://kb.epam.com/download/attachments/1755616936/mssql-jdbc-8.4.0.jre11.jar?version=1&modificationDate=1658487873772&api=v2)
and [spark-mssql-connector_2.12_3.0-1.0.0-alpha](https://kb.epam.com/download/attachments/1755616936/spark-mssql-connector_2.12_3.0-1.0.0-alpha.jar?version=1&modificationDate=1658487875128&api=v2). Place them into spark/jars, for example:
```shell
/home/<user_name>/raw-microsoft-wwi/.venv/lib/python3.8/site-packages/pyspark/jars
```

### 7 Run the database
#### 7.1 First launch:
Download WideWorldImporters database files and restore them within the docker image
```shell
$ make restore
```
After DB restoring docker container is already up and connected to 1433 port with credentials you set in “.env”. Please check multi-container group “epmadpafic_raw” running with 4 containers:
- “zookeeper-1”
- “mssql”
- “kafka-1”
- “connect-1”  
![linux_containers.png](img/linux_containers.png)
#### 7.2 Second and further docker container starting
```shell
$ make up
```

### 8 Run the “raw-microsoft-wwi” project
If you want to load data using batch processing via JDBC connection to the database:
```shell
$ poetry run console load -t 2018
```
and you will get delta tables with a raw data filtered till 2018-01-01 on path you set into “.env” by environment variable named “DELTA_TABLES_DEST”  
![linux_delta.png](img/linux_delta.png)

If you want to load data using streaming from Kafka:
```shell
$ poetry run console load -m debezium
```
**Note:** At the first run using streaming from local Kafka, you may need to create Debezium connector by adding --create_connector in command:
```shell
$ poetry run console load -m debezium --create_connector
```
Before running a command, make sure that you have provided "KAFKA_CONNECT_PORT" and "KAFKA_CONNECT_URL" variables in .env file </br>

If you want to change streaming source from Kafka to Azure Eventhubs:
```shell
$ poetry run console load -m debezium -s eventhubs
```