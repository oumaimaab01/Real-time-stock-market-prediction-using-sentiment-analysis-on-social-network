Ce projet a démontré le potentiel du Big Data et de l’analyse des sentiments pour anticiper les variations des actions de Tesla en temps réel. En combinant Apache Kafka, Spark et Elasticsearch avec des modèles LSTM, il a permis de relier données sociales et financières. Malgré son efficacité, des améliorations sont possibles, comme l’élargissement des sources de données et l’intégration de facteurs externes. Ces avancées pourraient affiner les prédictions et ouvrir la voie à des applications financières plus avancées, telles que l’optimisation des portefeuilles et la gestion des risques.

### Architecture de la solution
L’architecture utilise Kafka pour l’ingestion de données provenant de Yahoo Finance et Reddit.
Les données sont traitées par Apache Spark, puis stockées dans Elasticsearch. Les modèles de
prédiction sont entraînés et suivis avec MLflow, et les visualisations sont effectuées avec Kibana. Les
tâches sont orchestrées par Apache Airflow, et les données volumineuses sont gérées avec Hadoop
dans un environnement conteneurisé.

![Architecture de la solution](architecture.png)
