# EatFoodWise (Restaurant quality metrics aggregator)

## Vision

- Goal of the project is to build a data pipeline aggregating various data points pertaining to restaurants collected from a variety of data sources and providing analytical insights into restaurant quality over a timeline.

- While choosing a restaurant, **customers** look at various quality metrics(ambience, taste, service, hygiene) and a lot of review sites to make their decision. It is also commonly observed that restaurant quality varies for better/worse over a timespan. Our goal is to visualize these metrics in an easily observable way to help customers with their choices. The restaurant insights  are also informative to **restaurant owners/corporate chains** to come up with action items to improve restaurant performance.
- Not all metrics are available from all the data sources. For example, restaurant hygiene related data points are gathered from data published by city health departments. Note: Extraction of data, not available via apis, can be done via web scraping etc. Work pertaining to this is considered out of scope of this project.


## Model

The data lake consists of foundational fact, dimension, and aggregate tables developed using dimensional data modeling techniques that can be accessed by engineers and data scientists in a self-serve manner to power data engineering. The ETL (extract, transform, load) pipelines that compute these tables are thus mission-critical.Our aim is to have data freshness and give engineering efforts that process data as quickly as possible to keep it up to date.
In order to achieve the data freshness in our ETL pipelines, a key challenge is incrementally updating these modeled tables rather than recomputing all the data with each new ETL run. This is also necessary to operate these pipelines cost-effectively.   In this project, we aim to achieve an incremental data processing model which is efficient and cost-effective at the same time.

## Insights 

### How are restaurant reviews correlated to health grade
<br>
<img src="images/reviews_vs_hygiene.gif" width="800" height="350" />

- Various restaurant metrics representing quality could be correlated. For example, a restaurant with consistent low health grades is very likely to also have bad review ratings. It will be helpful for the customers when there is a visualization of correlation between various data points like restaurant rating vs hygiene.
 <br>

### Restaurant quality over a timeline
  <br>
  <img src="images/restaurant_quality-timline.gif" width="800" height="350" />

  - Customers might want to know how a restaurant has fared over a specific time period. It is not uncommon for restaurant quality to take a sharp turn for better or worse due to various reasons(management change, cost cutting measures, stress on profits etc). These changes will not be reflected over average review ratings provided by various review sites.
  <br>
### Review count per cuisine per site
<br>
<img src="images/review_counts_per_site.gif" width="800" height="350" />

  -We cannot simply trust the average review value alone. Different review sites have different counts of reviews for a particular restaurant. It is helpful to see the distribution of review counts per cuisine per review_site. Outliers in ratings collected from different review sites could indicate potential fraud.
<br>

## Design

![Getting Started](./images/foodieViews.svg)

- Restaurant quality metrics are aggregated from various data sources(google, yelp reviews, health department grades etc).
- Data ingestion into the system is  done using various triggers
  - AWS cloudwatch cron: Raw data uploaded to S3 is processed as micro batches using glue jobs.
  - On-demand triggers for glue jobs
- Data collected is fed into data processing pipeline, built off [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)(serverless data integration and ETL tool) using [S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)(AWS's object storage service) as the storage layer.
- Data post extraction/transformation is exposed for interactive analytics on [Aws Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)(Presto based distributed query engine). 
- Athena service is integrated with [Apache Superset](https://superset.apache.org/docs/intro/) to provide further data exploration and visualization capabilities.

- The data visualized using Apache Superset  is hosted on [DigitalOcean droplet](https://docs.digitalocean.com/products/droplets/) as a cost-effective method to create and share the visulization.



<!-- ## [Dashboard](TBD)
<img src="images/dashboard.gif" width="900" height="450" /> -->

<br>

## Setup

The raw data for health_grade, google_reviews and yelp_reviews were generated locally and were inserted into AWS S3's input bucket. The raw data of the reviews were designed in such a way that the review corresponding to a particular review_id could be updated in cases where reviewer has a change of mind and updated the review to a new value. The design is based on an incremental data processing model. New data is uploaded on a regular interval in the raw data bucket. When new data is added, a trigger is initiated causing Glue job run and therby process the data. The trigger could be scheduled trigger, conditional trigger or on-demand trigger. In all cases, the glue job runs and data is processed.

The glue job processes the data, modeling it into form that could be further used to perform meaningful queries and to visualize the data. AWS Athena is the serverless interactive analytics service we use to run meanigful queries on the processed data tables thereby giving insights on the restaurants.

We can visualize the results of our query and more in our visualization platform, which in our case is, Apache Superset. Being an open source visualization platform, Superset offers a cost-effective way to visualize our data and interact with it. We can plot the correlation between health_grade and customer reviews, the changes in customer reviews as well as health_grade  of a restaurant over a period of time and much more. This could help not just the consumers while choosing a restaurant , but the business owners as well in improving the quality and customer experience.
The Apache superset dashboard is hosted on a DigitalOcean droplet as yet another step towards cost effectiveness.
