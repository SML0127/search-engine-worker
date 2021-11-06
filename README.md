# Worker of Distributed Web Crawler and Data Management System for Web Data 


## What we provide
- Crawl and Parse product data in distributed environment (Master / Worker model).
- Upload / Update crawled data in the database incrementaly (View maintenance in Database).

   (Update example)
<img width="400" height="500" alt="overall_architecture" src="https://user-images.githubusercontent.com/13589283/140600455-fc2c143e-9d12-4c8c-984f-e1d9b082c9fb.jpg">

- Upload / Update crawled data to target sites (View maintenance in target sites).
- Register schedule for crawling and view maintenance.

------------
## How to support
- Provide all services through GUI.
   - git repository link: https://github.com/SML0127/pse-extension
- For crawling in distributed environment, we used Breadth-First-Search Crawling Model and Redis & RQ as a Message Broker.
- For Breadth-First-Search Crawling Model, we create several operators for crawling.
- [Docker](https://www.docker.com/) image for our ubuntu environment
   - git repository link for Master: https://github.com/SML0127/pse-master-Dockerfile
   - git repository link for Worker: https://github.com/SML0127/pse-worker-Dockerfile


------------
## What languages, libraries, and tools were used?
- Mainly based on Python
- Python Flask for Web Application Server and DB Server
- PostgreSQL for Database
- [Apachi Airflow](https://airflow.apache.org/) for Scheduling
- [Redis](https://redis.io/) & [RQ](https://python-rq.org/) for Message Broker in distributed environment
- [Selenium](https://www.selenium.dev/) & [Chromedriver](https://chromedriver.chromium.org/downloads) for Crawling


------------
## Demo videos
- Crawling


https://user-images.githubusercontent.com/13589283/140605217-df4290e2-34d9-4207-ad2d-0c8164c7ce03.mp4


- Upload / Update crawled data (View maintenance in Database).


https://user-images.githubusercontent.com/13589283/140605266-5f5999bd-fd8d-4595-9931-f0504113ecdb.mp4



- Upload / Update crawled data to target sites (View maintenance in target sites).

https://user-images.githubusercontent.com/13589283/140605229-4057f834-82d0-4917-8e39-a60aceb9599f.mp4



