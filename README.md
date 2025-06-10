### Steps to reproduce the env:

1. ```eval $(minikube docker-env) && minikube addons enable metrics-server```

2. ```kubectl create namespace kafka && kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka```

- Wait for it to be running, and then:

3. ```kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka```

- Wait for it to be running, adn then:

4. ```
    kubectl create secret generic minio-creds \
    --from-literal=accessKey=minioadmin \
    --from-literal=secretKey=minioadmin \
    --namespace default
  ```

5. ```
    kubectl apply -f minio-deployment.yaml
    ```

- Wait for it to be running, and then:

6. ```
    eval $(minikube docker-env) && cd trans-worker && docker build -t audio-transcription-service:v1 . && cd .. && cd ingestion-service && docker build -t youtube-ingestion-service:v1 . && cd ..
    ```

- Finally:

7. ```kubectl apply -f deployments.yaml```