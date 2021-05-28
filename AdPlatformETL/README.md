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

We have categorized the Project in multiple sections and coded the sections as per their workflow:
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
**_Code is attached_**

### Ad Server
The Ad Server is responsible for serving the ads to the users. When a user is active online, the client application will send a request to the Ad server along with the user details. The request from a typical mobile app client will contain a Google Play Store ID (GPID) or an Apple ID along with locality details. Upon receiving the request, the Ad server will execute the following events:
  - Find the list of available Ads which needs to be fetched by querying MySQL with user attributes.
  - Hold an auction among candidate Ads and the winning Ad will be served to the user.

























## Datasets used in the Project
1. [Amazon Advertisements](https://www.kaggle.com/sachsene/amazons-advertisements): This data set contains data pertaining to the advertisement from Amazon (stand by the end of 2019)
2. [ADS 16 data set](https://www.kaggle.com/groffo/ads16-dataset): This data set was used to ascertain the user preference for the advertisement data. 
3. [Advertising](https://www.kaggle.com/tbyrnes/advertising): This data set contains the demographics and the internet usage patterns of the users.
