# Cloud Project: Compare performance of Hadoop vs Spark
## Rest API to run SQL query on large set of data using Hadoop and Spark.
### Requirements:
- Python 3
- Hadoop
- Hadoop Streamer Path
- HDFS CSV Files Path

### Installation:
1) Create a virtualenv `Cloud` and activate it. If you do not have virtualenv installed, install it. (Installation: [Windows](https://thinkdiff.net/python/how-to-install-python-virtualenv-in-windows/), [Linux & MAC OS](https://medium.com/@garimajdamani/https-medium-com-garimajdamani-installing-virtualenv-on-ubuntu-16-04-108c366e4430)) 
2) In the project home folder, Install the necessary packages using the command `pip install -r requirements` .
3) Create `config.py` in `cloudproject` folder.
4) Add the following code to `config.py`
    ```python
    HADOOP_STREAMER_PATH = "<Path of the Hadoop Streamer JAR file>"
    HDFS_CSV_PATH = "<Path of the CSV Files in HDFS>"
    ```
5) Execute the following command to apply migrations: `python manage.py migrate`.
6) To run the server in localhost with default port, execute the command `python manage.py runserver`. 

### Usage:
This REST API doesn't have any authentication. To access this API, send a **POST** request using any request managers like Postman with data having key `query` with value as the **SQL query**.

The output of the query contains two keys: `mapreduce` and `spark`. Each key contains the **time** it took to execute the query on the specified data and the **result of the query**.

You can learn more about APIs and how to use them [here](https://schoolofdata.org/2013/11/18/web-apis-for-non-programmers/). 