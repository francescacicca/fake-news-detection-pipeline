# Real-Time Fake News Detection Pipeline

A real-time stream-processing pipeline designed to ingest live news headlines via **RSS feeds**, classify them as real or fake using a purposefully trained classification model, and visualize the results on a live dashboard using Grafana. The entire application stack is containerized and orchestrated with Docker.





## Architecture Overview

* **News Producer (Python):** Continuously polls new articles from major RSS feeds, and sends them as messages to the streaming pipeline.
* **Apache Kafka broker (KRaft mode):** Acts as the central event broker, buffering the incoming stream of news articles to decouple data collection from downstream analysis.
* **Apache Spark:** Consumes the live stream in micro-batches, processes the text, and applies a pre-trained classification model to predict whether each headline/article is real or fake along with a confidence score.
* **PostgreSQL:** Serves as the relational storage layer, holding all classified articles, timestamps, and model predictions.
* **Grafana:** Connects directly to the database to display real-time analytics, distribution charts, and a live monitoring feed of incoming news.





## Machine Learning & Training Pipeline

The classification model was trained and evaluated offline on the [Fake News Detection Dataset (6000 News Articles) by Mobeen Fatima](https://www.kaggle.com/datasets/mobeenfatimah/fake-news-detection-dataset-6000-news-articles/code), which provides labeled examples of both authentic and fabricated news stories. 

For this purpose, the standard **80/20 split** was chosen, allocating 80% of the data to train the model and reserving 20% to evaluate its performance.

The pipeline tokenizes the text, filters out common stop words, converts tokens into numerical frequency vectors using term hashing, applies TF-IDF scaling, and trains a **Logistic Regression classifier**.

Once trained, the entire pipeline is saved to a shared Docker volume, allowing the Spark streaming processor to load it directly and run inference on incoming live articles without training delays.



### Performance

Evaluated on the held-out 20% test partition, the pipeline achieved the following evaluation metrics:

* **Accuracy**: 98.93%
* **Weighted F1-Score**: 0.9893
* **Weighted Precision**: 0.9893
* **Weighted Recall**: 0.9893





## Project Structure

```text
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py           # Ingestion script for RSS feeds
├── spark_processor/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── train.py              # Offline model training script
│   └── processor.py          # Real-time streaming and inference engine
├── grafana/
│   └── dashboard/
│       └── dashboard.json    # Exported Grafana visualization template
├── model/                    # Persisted PipelineModel directory
├── data/
│   └── news_dataset.csv      # Kaggle dataset for training
├── docker-compose.yml        
└── README.md
```





## Getting Started

### Prerequisites

* Docker Desktop installed and running
*  [Dataset from Kaggle](https://www.kaggle.com/datasets/mobeenfatimah/fake-news-detection-dataset-6000-news-articles/code) downloaded  `news_dataset.csv` 



### Installation

* Clone the Repository
* Setup Docker Environment
* Proceed to Training phase



#### Model Training

If the `./model/` folder is empty or you want to retrain the pipeline with new data:

1. Place `news_dataset.csv` inside the `./data/` directory.
2. Run the training job inside an ephemeral container:
   ```bash
   docker compose run --rm spark-processor python train.py

3. Once completed, the trained `PipelineModel` will be saved into `.model/spark_fake_news_detection_pipeline`.



### Starting

Launch Docker Desktop and start all services in detach mode:

```bash
docker compose up -d --build
```

Verify that all containers are running with no issues:

```bash
docker compose ps
```



### Data Visualization (Grafana)

1. Open your browser and navigate to `http://localhost:3000`.
2. Log in using default credentials (`admin` / `admin`).
3. Navigate to **Connections** > **Data sources** > **Add data source** and select **PostgreSQL**.
4. Enter the connection settings:
   * **Host:** `postgres:5432`
   * **Database:** `news_db`
   * **User:** `admin`
   * **Password:** `adminpassword`
   * **SSL Mode:** `disable`
5. Click **Save & test** to verify the connection.



If needed, a *ready-to-use* example dashboard is included in `./grafana/dashboard/dashboard.json`. To load it: 

* Go to **Dashboards** > **New** > **Import**
* Click **Upload dashboard JSON file** and select the file.
* Select your configured **PostgreSQL** data source from the dropdown and click **Import**.



