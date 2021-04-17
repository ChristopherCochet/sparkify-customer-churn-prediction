# Sparkify - Predicting customer churn for a popular music streaming service  

**Project description:** In this project we predict customer churn on a large user behavior dataset using Spark and Databricks.

Two datasets are provided: 
 * a mini dataset that we use to explore and get an understanding of the dataset's characteristics and features 
 * a large dataset (12GB), for which Spark and Hadoop are required to manipulate - we use the databricks big data ecosystem (PySpark, Spark SQL, Spark ML, ML Flow) to do so. 

<kbd> <img src="images/music_service.png/?raw=true"/> </kbd>

<ins>1. In a first phase we focus on the Sparkify mini dataset :  </ins>\
  [ ] Load and perform EDA on the mini dataset answering business questions as we go along \
  [ ] Identify customer that have churned  \
  [ ] Transform the dadaset with the appropriate features and label so that supervised machine learning framework can be used  

  <ins>2. With a better understanding of the dataset, in a second phase, we turn our attention to the large Sparkify dataset to perform churn predictions  :  </ins>\
  [ ] Load and perform EDA on the large dataset \
  [ ] Pre-process the dataset, feature engineer and labels  \
  [ ] Explore the processed dataset and perfom survival analysis \
  [ ] Model and predict customer churn using Spark's ML libraries \
  [ ] Evaluate models and assess results 


For reference: All databricks html notebooks can be found below:
* [mini dataset-EDA](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/mini-dataset/1-Mini_Sparkify-Load_and_Investigate.html)
* [mini dataset-preprocessing](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/mini-dataset/2-Mini_Sparkify-Preprocessing.html)
* [large dataset-EDA](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/large-dataset/1-Large_Sparkify-Load_Dataset_and_Investigate_EDA.html)
* [large dataset-preprocessing](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/large-dataset/2-Large_Sparkify-Preprocessing.html)	
* [large dataset-modelling](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/large-dataset/3-Large_Sparkify-Modelling_and_Evaluation.html)

---

# Background: The Sparkify streaming service and Databricks

## Sparkify 
Sparkify is a music streaming dataset where users can use a free version of the service or use a paid susbcription. For each users, online interactions with the services are recorded along with their timestamp. 


<kbd> <img src="images/sparkify-background.PNG/?raw=true"/> </kbd>

## Databricks

  > Databricks is the implementation of Apache Spark on cloud (Microsoft Azure, Amazon AWS or GCP). With fully managed Spark clusters, it is used to process large workloads of data and also helps in data engineering, data exploring and also visualizing data using Machine learning. It runs a distributed system behind the scenes, meaning the workload is automatically split across various processors and scales up and down on demand. 


<kbd> <img src="images/databricks-ecosystem.png/?raw=true"/> </kbd>


  reference: https://databricks.com/product/faq/


  <ins>Some key features of Databricks</ins>:
  * **Databricks Workspace** – interactive workspace that enables data scientists and data engineers to collaborate and work closely together on notebooks and dashboards
  * **Databricks Runtime** – includes Apache Spark and support Scalla, PySpark, Python and Spark SQL amongst languages
  * **Fully managed service** - resources like storage, virtual network cluster and compute are easy to start and stop
  * **Databricks File System (DBFS)** – abstraction layer on top ofcloud object storage (blob, S3 ect.)  

  We will be using Databricks Community Edition to analyze the Sparkify dataset and model customer churn. 

  
<kbd> <img src="images/Databricks Community Edition.gif/?raw=true"/> </kbd

---

  # 1. The Sparkify Mini Dataset - Load the mini dataset and perform EDA answering service usage
  
  ## Dataset Characteristics  
  * The mini sparkify dataset contains music choices from 225 customers over a 60 days period. The size of this mini dataset is 286,500 rows. 
  
  <ins>The dataset is composed on the following features and can be thought of as a sequence of service user events and music listened </ins>:
  ```
  artist: string - artist name
  auth: string  - authentification method
  firstName: string - user first name
  gender: string - user gender
  itemInSession: long
  lastName: string - user last name
  length: double - length of the song listened
  level: string - sparkifiy user service level (paid or free)
  location: string - location of the user
  method: string - http service method
  page: - user service interaction event 
  registration: long - timestamp of user service registration 
  sessionId: service session id
  song: song name played by the user
  status: - http status
  ts: long - timestamp of user service event 
  userAgent: - web browser used
  userId: string - unique userid 
```

  <ins>The dataset's features can be broken down into four categories </ins>:
  * **User information** : userid, first name, last name, gender, location
  * **Sparkify service information** : level, registration
  * **User service event/interactions information** : method, iteminsession, page, status, ts, useragent, sessionid
  * **Songs & artist informations** : artist, song, length
  
  Below is a sample of a user's interactions with the service order by timestamp:

    <kbd> <img src="images/user-sample.png/?raw=true"/> </kbd>

  ## Perform EDA and answer service usage questions

  ### Sparkify mini dataset EDA Notebook
  
  <kbd> <img src="https://christophercochet.github.io/Market-Basket-Analysis/images/jupyter.png"/> </kbd>
  Refer to the following notebook [here](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/mini-dataset/1-Mini_Sparkify-Load_and_Investigate.html)

  * **What is the distribution of of users by gender ?**
  ```
  Users are fairly well balanced by gender, 212 males vs 104 females
  ```

  * **How many users were paying for the service using the first and last event recorded forach user in the dataset**
  ```
  Out of 225 users, 48 (22 %) had a paid subscription at the earliest date recorded in dataset vs 177 (88 %) users which did not (free subscription)
  Out of 225 users, 145 (65 %) had a paid subscription at the latest date recorded in dataset vs 80 (35 %) users which did not (free subscription)
  ```

  * **Which users listened to the most songs ?**
  ```
  user 39 listened to ~6k songs in two months and average of 100 songs per days !
  users 92 and 140 listened to ~4,500 songs over two months
  ```

  * **Where are the most service users located ?**
  ```
  LA & NY are where most of service users are located
  ```

  * **How many unique artists & songs does the dataset have ?**
  ```
  The dataset holds approx. 17K artists and 58k songs
  ```

  * **What are the most popular songs ? arists ?**
  ```
  The most populat songs are **1 - You're The One by Dwight Yoakam** , **2 - Undo by Bjork** , **3 - Revelry by Kings Of Leon**
  The most populat artists are **1 - Kings Of Leon** , **2 - Coldplay** , **3 - Florence + The Machine**
  ```

  * **Which users listened to the most songs ?**
  ```
  user 39 listened to ~6k songs over a two months period, average of 100 songs per days```
  users 92 and 140 listened to ~4,500 songs over two months
  ```

  ## Tracking our progress
  [X] Load and perform EDA on the mini dataset answering business questions as we go along \
  [ ] Identify customer that have churned  \
  [ ] Transform the dadaset with the appropriate features and label so that supervised machine learning framework can be used  

---

# 2. The Sparkify Mini Dataset - Define Churn and Identify Customer that Have Churned
  
  ## How should a user churn be defined ?

  The relevant features to identify whether a customer has changed are **page** and **level** features of the data sets.
  ```
  level : possible values  -> ['free', 'paid'] 
  ```
  ```
  page : possible values  -> ['Cancel', 'Submit Downgrade', 'Thumbs Down', 'Home', 'Downgrade', 'Roll Advert', 'Logout', 'Save Settings', 'Cancellation Confirmation', 'About', 'Settings', 'Add to Playlist', 'Add Friend', 'NextSong', 'Thumbs Up', 'Help', 'Upgrade', 'Error', 'Submit Upgrade'] 
  ```

> **Submit Downgrade** and **Cancellation Confirmation** event type seem be good proxies to identify whether a customer has churned or not
    
  Lets look at what happens before and after a **Submit Downgrade** event occurs for a given user (userid 100015):
  1. the user has a paying subscription
  2. the user goes to the downgrade page
  3. the user submits downgrade request
  4. the user has now a free subscription

  <kbd> <img src="images/submit-downgrade.PNG/?raw=true"/> </kbd>

  Lets look at what happens before and after a **Cancellation Confirmation** event occurs for a given user (userid 100015):
  1. the user has a paying subscription
  2. the user goes to the downgrade page
  3. the user submits Cancellation Confirmation
  4. the user has now ended the sparkify subscription

<kbd> <img src="images/cancel-subscription.PNG/?raw=true"/> </kbd>

  ## Tracking our progress
  [X] Load and perform EDA on the mini dataset answering business questions as we go along \
  [X] Identify customer that have churned  \
  [ ] Transform the dadaset with the appropriate features and label so that supervised machine learning framework can be used  

---

# 3. The Sparkify Mini Dataset - Transform the dataset with the appropriate features and label so that supervised machine learning framework can be used

In the following notebook, we label, transform and feature engineer the orginal dataset with each row corresponding to a user. The label **churn** flags whether this user has churned based on the chrun definition discussion above.

<kbd> <img src="https://christophercochet.github.io/Market-Basket-Analysis/images/jupyter.png"/> </kbd>
Refer to the following preprocessing notebook [here](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/mini-dataset/2-Mini_Sparkify-Preprocessing.html)

Steps followed:
  1. aggregate the data for each user, drop features uncorrelated to churn
  2. engineer features features - count of events, songs for each user, tenure in days and time from registration to first use
  3. label each user (row) a binary **churn** features  
  3. clean the data set and removing rows with null value
  4. save the clean dataset in a Spark table



  <ins>**The sparkify transformed labeled dataset ready a supervised binary classification is described below:** </ins>:
  ```
    userId                  object  - unique user id
    gender_first            object  - user gender
    song_count              int64   - count of songs played by the user
    song_nunique            int64   - count of unique songs played by the user
    sessionId_count         int64   - count of session ids
    length_sum              float64 - total song playing time 
    time_to_first_use_days  float64 - number of days between registration and first use
    service_tenure_days     float64 - number days the user used the service
    About                   int64  - number of time the About page was visited or event took place for the user
    Add_Friend              int64  - number of time the Add_Friend page was visited or event took place for the user
    Add_to_Playlist         int64  - number of time the Add_Friend page was visited or event took place for the user
    Cancel                  int64  - number of time the Cancel page was visited or event took place for the user
    Downgrade               int64  - number of time the Downgrade page was visited or event took place for the userr
    Error                   int64  - number of time the Error page was visited or event took place for the user
    Help                    int64  - number of time the Help page was visited or event took place for the userr 
    Home                    int64  - number of time the Home page was visited bor event took place for the user 
    Logout                  int64  - number of time the Logout page was visited or event took place for the user 
    NextSong                int64  - number of time the NextSong page was visited or event took place for the user
    Roll_Advert             int64  - number of time the Roll_Advert page was visited or event took place for the user 
    Save_Settings           int64  - number of time the Save_Settings page was visited or event took place for the user 
    Settings                int64  - number of time the Settings page was visited or event took place for the user
    Submit_Upgrade          int64  - number of time the Submit_Upgrade page was visited or event took place for the userr 
    Thumbs_Down             int64  - number of time the Thumbs_Down page was visited or event took place for the user
    Thumbs_Up               int64  - number of time the Thumbs_Up page was visited or event took place for the user 
    Upgrade                 int64  - number of time the Upgrade page was visited or event took place for the user
    churn                   object - whether the user has churned or not
  ```

A sample of the final user table is shown below:


<kbd> <img src="images/user-labeled-table.PNG/?raw=true"/> </kbd>


## Tracking our progress
[X] Load and perform EDA on the mini dataset answering business questions as we go along \
[X] Identify customer that have churned  \
[X] Transform the dadaset with the appropriate features and label so that supervised machine learning framework can be used  

---

# 4. Sparkify Large Dataset - Perform EDA, preprocess and predict customer churn

With a better understanding of the sparkify mini dataset and features, we now perfom the same EDA and transformation on the large dataset

## Dataset Characteristics  
The large sparkify dataset contains music choices from 22,278 customers over a 60 days period. The size of this mini dataset is 26,259,199 rows. 

## Perform EDA on the large dataset 

<kbd> <img src="https://christophercochet.github.io/Market-Basket-Analysis/images/jupyter.png"/> </kbd> 
Refer to the following html notebook [here](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/large-dataset/1-Large_Sparkify-Load_Dataset_and_Investigate_EDA.html)

Key dataset highlights:

* **What is the distribution of of users by gender ?**
  ```
  Similar to the mini dataset, Users are  well balanced by gender, 11.6k males vs 10.6k females
  ```

* **Which users listened to the most songs ?**
  ```
  user 1564221 listened to ~8k songs in two months, anavegare of 132 songs per days!
  ```

* **Where are the most service users located ?**
  ```
  Similar to the mini dataset, LA & NY are where most of service users are located
  ```

* **Which users listened to the most songs ?**
  ```
  user 1564221 listened to ~8k songs in two months, anavegare of 132 songs per days
  ```

## Tranform the large Sparkify dataset 

We follow the steps used to preprocess the Sparkify mini dataset and apply these for the large dataset 
 
<kbd> <img src="https://christophercochet.github.io/Market-Basket-Analysis/images/jupyter.png"/> </kbd> 
Refer to the following html notebook [here](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/large-dataset/2-Large_Sparkify-Preprocessing.html)


## Perform EDA on the tranformed labeled dataset

* **What propotion of Spakify customers have churned ?**  
```
Overall, 40% of Sparkify users have churned in the dataset. We therefore won't need to deal with an imbalanced dataset while modelling. 
```

<kbd> <img src="images/churn-proportion.PNG/?raw=true"/> </kbd>

* **Compute average user tenure by gender**
```
* The average tenure in the dataset is 40 days and very similar between males and female Sparkify users
* The tenure distribution is left skewed but we can see a peak very early with over 1200 users that seem to have quit on the first day of usage
```

<kbd> <img src="images/tenure-distribution.PNG/?raw=true"/> </kbd>

* **What does the progression of churn over time look like (survival analsyis)?**  
```
From the survival chart we can see that:
* 40% of customers have churned over a 2 months period
* Male and Female survival plots and churn profiles are very similar
* Sparkify seems to lose customer at faster clip within the first couple of days and then after the ~55 day mark where the curve is much steeper
* It takes approx. 55 days to reach 30% of user churn and then 5 days to see 10% more users churn
```

<kbd> <img src="images/survival-analysis.PNG/?raw=true"/> </kbd>

## Model and predict customer churn 

In the following notebook, we use the labeled dataset to fit models and evaluate churn predictions for 4 Spark ML algos. This a binary classification where the proportion of customer churned in the transformed dataset is well balanced as disucssed in the previous section.

<kbd> <img src="https://christophercochet.github.io/Market-Basket-Analysis/images/jupyter.png"/> </kbd>
Refer to the following html notebook [here](https://raw.githack.com/ChristopherCochet/sparkify-customer-churn-prediction/main/databricks-notebooks/large-dataset/3-Large_Sparkify-Modelling_and_Evaluation.html)

<ins>Steps followed</ins>:
  1. This is a binary calssification - an appropriate evaluation metric that we used is ROC-AUC (**areaUnderROC**)
  2. Use spark pipelines to transform features using Spark's **VectorAssembler** which combines a  list of columns into a single feature vector
  3. Split the processed dataset into train and a test sets 
  4. Evaluate four ML models:  LogisticRegression, RandomForestClassifier, LinearSVC and GBTClassifier
  5. Assess feature importance


## Assess results

All classifiers reported a ROC score above 90% with the Gradient Boosted Tree model recording the best perfomance with ~0.97 ROC

```
  Training model : lr_classifier 
  Pipeline : lr_classifier areaUnderROC score 0.9402484122969708
  **************************************************************
  Training model : rf_classifier
  Pipeline : rf_classifier areaUnderROC score 0.9615282227093049
  **************************************************************
  Training model : gtb_classifier
  Pipeline : gtb_classifier areaUnderROC score 0.972535781861781
  ***************************************************************
  Training model : lsvc_classifier
  Pipeline : lsvc_classifier areaUnderROC score 0.9475858952022346r
```

## Feature importance

<kbd> <img src="images/feature_importance.PNG/?raw=true"/> </kbd>

<ins>For the Boosted Tree model, the features most important for the user churn classification were</ins>:
1. The **Cancel** event count
2. The **Submit Upgrade** event count
3. The **Downgrade** event count
4. The **Roll Advert** event count

These seem to mak sense as their are a direct reflection of the user's sentiment about the Sparkify service. Suprisingly, **service tenure** seem to have little importance on the binary classification. 

## Tracking our progress
  [X] Load and perform EDA on the large dataset \
  [X] Pre-process the dataset, feature engineer and labels  \
  [X] Explore the processed dataset and perfom survival analysis \
  [X] Model and predict customer churn using Spark's ML libraries \
  [X] Evaluate models and assess results 

---

# 5. Conclusion 

We used the mini dataset to get a good understanding of the Sparkify service and users. This helped us define, identify customer churn and how to transform the raw data.

We were then able to apply the necessary transformations to the large dataset and label the data so that we could apply a supervised binary classification approach to predicting customer churn. We seem to have obtained very good resuts with an ROC 0f ~0.97.  

Using the Databricks ecosystem was a great way to ease in the big data and Spark ecosystem. In addition we used Spark MLflow to track, save and inventory the models and experiment results obtained along the way.  
<kbd> <img src="images/MLflow.PNG/?raw=true" width="1000"> </kbd>

> MLflow is an open source platform to manage the ML lifecycle, including experimentation, reproducibility, deployment, and a central model registry.

<kbd> <img src="images/mlflow-overview.png/?raw=true"/> </kbd>

reference: https://mlflow.org/

