# Wide World Importers data ingestion into the Data Lake using Spark

## Project Description
“raw-microsoft-wwi” project is the part of [IACDA accelerator](https://kb.epam.com/display/EPMADPAF/IACDA+-+Infrastructure+as+Code+Data+Accelerator) data pipelines. It was designed using the sample database for the fictitious company [Wide World Importers](https://learn.microsoft.com/en-us/sql/samples/wide-world-importers-what-is?view=sql-server-ver16) and rely on the Apache Spark and use Delta as a data format for storing data. Project reads data from database tables and writes data into delta tables (ingestion part).  The logic of the pipelines is displayed on the schema below.  

![pipeline_schema.png](documentation/run_locally/img/pipeline_schema.png)  

This README describes how to work with the Project in your local environment. For more information on this data pipeline in general and instructions how to run it in Databricks please go to the [IACDA Databricks Data Pipelines](https://kb.epam.com/display/EPMADPAF/Databricks+Data+Pipelines).  

## Project repository main components

1. Guides for Project local run
- `/documentation` – guides for various operating systems: Linux, Windows 10, Windows 11 with WSL

2. Installation files
- `.env.dist` – distributive for environment settings (delta tables destination path etc.)
- `install.sh` – installation script
- `pyproject.toml`, `poetry.toml` – configuration files

3. Sources to restore the WideWorldImporters Database in docker container
- `Makefile`, `docker-compose.yml`, `restore_db.sql`

4. Project main resources
- `/src/raw_microsoft_wwi` – Schemas, Reader, Writer, Transformer, Consumer, Tests, Configuration files etc.
- `/src/run_locally.py` – script for Project launch

5. Project deploy tools
- `/azure_pipelines` – ".yml" pipelines to continuously test, build, and deploy Project code
- `/databricks_notebooks` and `/databricks_jobs` – resources for Project run in Azure Databricks


6. E2E Testing tools
- `/e2e-testing-test-data` - raw area e2e testing expected delta tables
- `/wwi-truncated-db` - Wide World Importers truncated database

## How to Install and Run the Project
Please, follow the guide that matches your operating system for local run:
- [Linux](documentation/run_locally/Ubuntu_22.04.md)
- [Windows 10](documentation/run_locally/Windows_10.md)
- [Windows 11 + WSL](documentation/run_locally/Windows_11.md)  

## How to Use the Project
Please note, that you have to build a `.whl` file before run the project "raw-microsoft-wwi" in Databricks. Wheels (.whl file) - component of the Python ecosystem that helps to make package installs just work. Open the local "raw-microsoft-wwi" project and run below command in console.

```shell
$ poetry build -f wheel 
```

As a result it creates a "dist" folder with `.whl` file. Then you have to open a `Databricks/Compute -> Your cluster -> Libraries -> Install new`. For more information about running in Databricks please go to the [IACDA Databricks Data Pipelines](https://kb.epam.com/display/EPMADPAF/Databricks+Data+Pipelines)  

## E2E Tests 
E2E testing is a part of Databricks CI/CD workflow. E2E tests is running automatically on deployment to Staging environment. See more details about CI/CD processes [here](https://kb.epam.com/pages/viewpage.action?pageId=1774421408) 

E2E tests is executing on truncated version of [Wide World Importers database](https://learn.microsoft.com/en-us/sql/samples/wide-world-importers-what-is?view=sql-server-ver16). 
To run E2E tests manually on Databricks, you have to:
1. Specify job run_mode parameter - is_e2e to True(by default it's False)
2. Specify database variables - db_url, db_username, db_password(by default using truncated version of World Wide Importers Database)
3. Specify raw_area_path variable - destination of actual delta tables 
4. Specify e2e_expected_data_delta_path variable - location of expected delta tables 
5. Wait till job execution will be finished :)

After execution you will get message with result of your tests. In case if tests failed - you will get detailed message with root cause of your fail.

For more detailed instructions how to run E2E tests in Databricks please visit [How to run E2E Tests on Databricks](https://kb.epam.com/display/EPMADPAF/How+to+run+E2E+Tests+on+Databricks)


## Troubleshooting
* If after the app running you got en Py4JJavaError `ModuleNotFoundError`.  
  It means Spark can't find the required jar files that he needs. Method to resolve. Try one of them in this sequence:    
  * Set `export PATH=$PATH:~/.ivy2/jars`
  * Then add the command above to your `~/.bashrc` file
  * Reboot your computer. It can help if you just installed Java.
  * Copy required jar files from `.ivy2/jars` to `.venv/lib/python3.8/site-packages/pyspark/jars`
  

* If you have an exception when trying to run the app locally:  
   _java.net.ConnectException: Call From <Host name> to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused_  
   It probably means that Spark trying save delta tables into HDFS by default.  
   To resolve it try to add the prefix `file://` before delta tables path in `.env` file.  
   E.g. `DELTA_TABLES_DEST=file:///<YOUR_PATH>/delta-tables/destination`

  
* If you got an error when trying to establish a connection to db like this:  
  _The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption.  
  Error: "PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target"._  
  To resolve this issue try to `trustServerCertificate=true` as an option in db connection URL in the `run_locally.py`

* If you got UnsatisfiedLinkError on windows then try to copy hadoop.dll (from downloaded before winutils folder) into windows/       system32 . 

* If you can't connect to Kafka broker and you are running Docker on Windows using WSL. <br />
  To resolve this issue you should uncomment line
  > -ADVERTISED_HOST_NAME=${KAFKA_HOSTNAME}

  in Kafka section in docker-compose.yml and set value of **KAFKA_HOSTNAME** to WSL IP in .env file. You can get IP address by running 
  ```shell
  $ hostname -I
  ``` 
  in WSL terminal <br />
  **Note**: WSL IP is dynamic, so you may need to change it after each reboot.