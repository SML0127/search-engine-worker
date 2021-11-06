# Worker of Distributed Web Crawler and Data Management System for Web Data 


## What we provide
- Create workflows for crawling.
- Crawl and Parse product data in distributed environment (Master / Worker model).
- Upload / Update crawled data in the database incrementaly (View maintenance in Database).
- Upload / Update crawled data to target sites (View maintenance in target sites).
- Register schedule for crawling and view maintenance.

<br>

------------
## How to support
- Provide all services through GUI.
   - git repository link: https://github.com/SML0127/pse-extension
- For crawling in distributed environment, we used Breadth-First-Search Crawling Model and Redis & RQ as a Message Broker.
- For Breadth-First-Search Crawling Model, we create several operators for crawling.
- [Docker](https://www.docker.com/) image for our ubuntu environment
   - git repository link for Master: https://github.com/SML0127/pse-master-Dockerfile
   - git repository link for Worker: https://github.com/SML0127/pse-worker-Dockerfile

<br>

------------
## What environment, languages, libraries, and tools were used?
- Master / Worker are run at Ubuntu 20.04.
- Mainly based on Python for Master / Worker.
- [React](https://reactjs.org/) & JSX for GUI.
- Python Flask for Web Application Server and DB Server
- PostgreSQL for Database
- [Apachi Airflow](https://airflow.apache.org/) for Scheduling
- [Redis](https://redis.io/) & [RQ](https://python-rq.org/) for Message Broker in distributed environment
- [Selenium](https://www.selenium.dev/) & [Chromedriver](https://chromedriver.chromium.org/downloads) & XPath for Crawling

<br>

## Overall Architecture
<br>
<img width="400" height="500" alt="overall_architecture" src="https://user-images.githubusercontent.com/13589283/140601538-9ebc134e-0e55-404e-9929-c231295de423.png">

## Overall Architecture with Implementaion
<br>
<img width="500" height="300" alt="overall_architecture" src="https://user-images.githubusercontent.com/13589283/140601624-d8bd5686-a8a9-4d40-baf9-c6376fb3c1cb.jpg">

<br>

------------
## Screenshots of GUI
- Create a workflow for crawling.
<img width="1635" height="400" src="https://user-images.githubusercontent.com/13589283/140605666-554aa847-a258-4a81-bf9d-31994bb48a26.png">


- Get XPath for parameter of operators in workfrow.
<img width="1635" height="400" src="https://user-images.githubusercontent.com/13589283/140606117-22d6f28b-574a-4452-b643-23776494d951.png">


- Save and load workflos.
<img width="1635" height="400" src="https://user-images.githubusercontent.com/13589283/140605668-54f0635d-8618-40a3-90be-72db7bd4c79d.png">


- Crawled data and Error Message.
<img width="1635" height="400" src="https://user-images.githubusercontent.com/13589283/140606249-e399f5dd-4ec5-4d1b-bc2b-09b68d211c8b.png">

<br>

------------
## Demo videos
- Crawling


https://user-images.githubusercontent.com/13589283/140605217-df4290e2-34d9-4207-ad2d-0c8164c7ce03.mp4


- Upload / Update crawled data (View maintenance in Database).


https://user-images.githubusercontent.com/13589283/140605266-5f5999bd-fd8d-4595-9931-f0504113ecdb.mp4



- Upload / Update crawled data to target sites (View maintenance in target sites).

https://user-images.githubusercontent.com/13589283/140605229-4057f834-82d0-4917-8e39-a60aceb9599f.mp4


