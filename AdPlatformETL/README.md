# Ad Platform ETL Pipeline

## Problem Statement
With increasing digitisation, there has been a tremendous boom in the field of online advertising as more and more companies are willing to pay large amounts of money in order to reach the customers via online platform. So, in this project, we built an ETL Pipeline for an online advertising platform.

## Technical Overview of Tasks to be performed in the Project
1. The platform will have an interface for campaign managers to run the Ad campaign and another interface for the client to present the Ads and send the user action back to the advertising platform.
2. Through the campaign manager, the Ad instructions (New Ad Campaign, Stopping the existing Ad campaign) will be published to a Kafka Queue. The ‘Ad Manager’ will read the message from that Kafka queue and update the MySQL store accordingly.
3. An Ad Server will hold the auction of Ads for displaying the Ad to a user device. The auction winner will pay the amount bid by the second Ad. A user simulator will hit the Ad Server API for displaying Ads and send the user interaction feedback back to the feedback handler through API.
4. Upon receiving the user interaction feedback, the feedback handler will publish that event to another Kafka queue.
5. The User feedback handler will collect the feedback through the feedback API and it will be responsible for updating the leftover budget for the Ad campaign into MySQL. It will also publish the feedback to the internal Kafka queue, which will be later published to HIVE through user feedback writer for billing and archiving.
6. Once the Pipeline is set, Multiple teams can use report generator to analyse the Ads data as per their requirement.

## Project Architecture
![image](https://user-images.githubusercontent.com/19205616/120000734-c306ef80-bff0-11eb-8173-b5615aaa75d8.png)

We have categorized the Project in multiple modules and coded the modules as per their workflow:
 - [Campaign Manager](#Campaign-Manager)
 - [Ad Manager](#Ad-Manager)
 - [Ad Server](#Ad-Server)
 - [Feedback Handler](#Feedback-Handler)
 - [Slot Budget Manager](#Slot-Budget-Manager)
 - [User Feedback Writer](#User-Feedback-Writer)
 - [Data Archiver](#Data-Archiver)
 - [Report Generator](#Report-Generator)

### Campaign Manager
From Campaign Manager interface we are receiving data from Kafka Queue in JSON format as below,
![image](https://user-images.githubusercontent.com/19205616/120003601-88528680-bff3-11eb-8223-b96aad0b7315.png)

### Ad Manager
In the Ad Manager, we used **PyKafka** consumer for reading the data from **Kafka** and **Python MySQL connector** for connecting & storing the data in MySQL.

### Ad Server
The Ad Server is responsible for serving the ads to the users. When a user is active online, the client application will send a request to the Ad server along with the user details. The request from a typical mobile app client will contain a Google Play Store ID (GPID) or an Apple ID along with locality details. Upon receiving the request, the Ad server will execute the following events:
  - Find the list of available Ads which needs to be fetched by querying MySQL with user attributes.
  - Hold an auction among candidate Ads using the [**‘Second-Price Auction’** strategy](https://www.inmobi.com/blog/2018/10/24/what-is-a-second-price-auction-and-how-does-it-work-video) and the winning Ad will be served to the user.
So ultimately, the responsibility of the Ad Server is to serve Ads to the user. This will be done through an API call. We used the Flask library to create a web server and serve APIs in Python.

**API Format:- **

<code>**HTTP Method: GET **
 http://localhost:5000/ad/user/6abc435e-0f72-11eb-8a4e-acde48001122/serve?device_type=android-mobile&city=mumbai&state=maharastra
</code>

**Sample API Response:-**

<code>
 {
         "text": "Jack Wolfskin Men's Rock Hunter Low Water
         Resistant Hiking Shoe",
         "request_id":"17001d26-0f72-11eb-8a4e-acde48001122"
}
 </code>

### Feedback Handler
The Feedback Handler is responsible for submitting user feedback. It will enrich the data before publishing it to the Kafka queue. We created a new Kafka topic for this purpose having **one partition** and a **replication factor of 1**.
The client application shares only the user interaction data in the feedback. The same needs to be combined with other attributes of the auction and the Ad campaign to make it efficient for consumption in billing and reports. When the client application sends the user feedback to the feedback handler through the feedback API, it will retrieve the extra attributes from MySQL to enrich the feedback data. The user feedback API will have the original Ad request identifier as an argument and that will be used for fetching the additional attributes.

Here, we used Flask for the APIs and MySQL connector library to connect to MySQL.
Once an Ad has been displayed, information on whether the user has clicked on the Ad, downloaded the advertised App or only viewed the Ad needs to be sent back to the Ad server through the user feedback API. The Feedback Handler will enrich the feedback data and publish it to the internal Kafka topic.

**API Format:-**

<code>
 HTTP Method: POST
 http://localhost:8000/ad/17001d26-0f72-11eb-8a4e-acde48001122/feedback
</code>

**Sample Request Body:-**

<code>
 {
     “View”:1,
     “Click”:1,
     “Acquisition”:0
 }
 </code>

**Sample API Response:-**

<code>
 {
    “status”:” SUCCESS”
 }
 </code>

### Slot Budget Manager
The purpose of the Slot BudgetMmanager is to distribute the leftover budget uniformly on the Ad slots and utilise the budget fully. As it needs to be working repeatedly and adjusting the Budget in a timely manner, so we scheduled it as a **Cron job** running every 10 minutes and the Python MySQL connector is used to write the code.

**Sample Cron Job:-**

<code>
*/10 * * * * /path/to/file/slot_budget_manager.py <database_host> <database_name> <database_username> <database_password>
</code>

### User Feedback Writer
The User Feedback Writer will read the user feedback messages from the internal Kafka queue(created in Feedback Handler module) and write the feedback data to HDFS(Hadoop Distributed File System) for archiving and billing purposes. This will be a **PySpark** consumer job.

### Data Archiver
Data Archiver is responsible for exporting Ads data from MySQL to Hive, for reporting & billing purposes. So we used Apache Sqoop command to import the data from MySQL to Hive, and it can be scheduled as a **CRON job** to import data in batch of durations(let's say every 10 mins).

**Sample Sqoop command:-**

<code>
sqoop import --connect jdbc:mysql://<MySQL_DB_Host>:3306/<mysql_db_name> --username <mysql_username> -P --table <mysql_table_name> --hive-import --create-hive-table --hive-database '<hive_db_name>' --hive-table '<hive_table_name_to_be_created>' -m 1
</code>

### Report Generator
HUE is used as a Report Generator, which is a User interface for the data stored in Hive.

## Technologies Used

[![python](https://user-images.githubusercontent.com/19205616/120016258-fdc55380-c001-11eb-81aa-bc0844797774.jpg)](https://www.python.org/)
 [![flask](https://user-images.githubusercontent.com/19205616/120016293-0e75c980-c002-11eb-9994-cf6994bbc881.jpg)](https://flask.palletsprojects.com/en/2.0.x/)
 [![kafka](https://user-images.githubusercontent.com/19205616/120016312-13d31400-c002-11eb-80cb-0f0ed694e36e.jpg)](https://kafka.apache.org/)
 [![hdfs](https://user-images.githubusercontent.com/19205616/120016328-1897c800-c002-11eb-9f80-165c956f994c.jpg)](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
 [![hive](https://user-images.githubusercontent.com/19205616/120016347-1c2b4f00-c002-11eb-9fa6-d8e58d621af1.jpg)](https://hive.apache.org/)
 [![hue](https://user-images.githubusercontent.com/19205616/120016352-1e8da900-c002-11eb-89ac-7f60975eec86.jpg)](https://gethue.com/)
 [![mysql](https://user-images.githubusercontent.com/19205616/120016359-20f00300-c002-11eb-8d4d-c5da216dc5c8.jpg)](https://www.mysql.com/)
 [![spark](https://user-images.githubusercontent.com/19205616/120016961-e3d84080-c002-11eb-9ad1-dc7e85b3350f.jpg)](https://spark.apache.org/)

## Datasets used in the Project
1. [Amazon Advertisements](https://www.kaggle.com/sachsene/amazons-advertisements): This data set contains data pertaining to the advertisement from Amazon (stand by the end of 2019)
2. [ADS 16 data set](https://www.kaggle.com/groffo/ads16-dataset): This data set was used to ascertain the user preference for the advertisement data. 
3. [Advertising](https://www.kaggle.com/tbyrnes/advertising): This data set contains the demographics and the internet usage patterns of the users.
