<div align="center">
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Kubernetes-f3f3f3?style=flat&logo=kubernetes&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Minio-61DAFB?style=flat&logo=minio&logoColor=%23000000&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Kafka-20BEFF?style=flat&logo=apachekafka&logoColor=%23fffff&logoSize=auto"></span>
<!-- <span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=%23ffffff&logoSize=auto"></span> -->
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Gemini-8E75B2?style=flat&logo=googlegemini&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/HuggingFace-040404?style=flat&logo=huggingface&logoColor=%23FFD21E&logoSize=auto"></span>
</br>
<div style="font-size: 2.5rem; margin-bottom: 1rem;"><strong><h1>What's the Topic? - An AI YouTube chapter generator</h1></strong></div>
</div>

## Project Overview

Well, YouTube's creator studio already has a baked in feature for generating chapters, but it is quite lack luster when it comes to granularity and control over the semantics of the chapters that it generates. Hence, this project exists.

It is still under development. This is the repo for the project's backend infrastructure.

## Local Development Setup

### Steps for local setup of the kubernetes/kafka backend server:

1. Start the Minikube server

    ```bash
    minikube start --driver=kvm --memory=10240mb --cpus=8
    ```

2. Enable the addons

    ```bash
    eval $(minikube docker-env) && minikube addons enable metrics-server
    ```

3. Create the kafka namespace and cluster

    ```bash
    kubectl create namespace kafka && kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
    ```

4. Then deploy the kafka-single-node bucket

    ```bash
    kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka
    ```

5. Create the minio keys. Replace the <access-key> and the <secret-key> with your desired keys.

    ```bash
    kubectl create secret generic minio-creds \
    --from-literal=accessKey=<access-key> \
    --from-literal=secretKey=<secret-key> \
    --namespace default
    ```

6. Deploy the minio storage bucket

    ```bash
    kubectl apply -f minio-deployment.yaml
    ```

7. Once the minio pod is running, create the docker images of all the services

    ```bash
    eval $(minikube docker-env) && cd trans-bridge && docker build -t audio-transcription-service:v1 . && cd .. && cd ingestion-service && docker build -t youtube-ingestion-service:v1 . && cd .. && cd trans-bridge && docker build transcription-bridge-service:v1 . && cd ..
    ```

8. After the images are done building, deploy the services

    ```bash
    kubectl apply -f deployments.yaml
    ```

9. Check the status of the services and the pods. Make sure all are in "Running" state.

    ```bash
    kubectl get pods --all-namespaces -w

    # and

    kubectl get svc --all-namespaces -w
    ```

10. Once all of them are running, forward all the ports. Grab the exact names of the pods from the previous command/s.

    ```bash
    kubectl port-forward svc/my-cluster-kafka-bootstrap 9092:9092 -n kafka
    
    kubectl port-forward svc/minio 9001:9001 -n default

    kubectl port-forward <youtube-ingestion-deployment-name> 8000:8000

    kubectl port-forward <transcription-bridge-deployment-name> 8001:8001
    ```

11. Finally, run the local-transcriber server and scale it horizontally as needed (run multiple instances in different tabs)

    ```bash
    cd local-transcriber && python3 app.py && cd ..
    ```

### Testing the chapter generation:

```bash
# URL
http://localhost:8000/process-youtube-url/

# METHOD
POST

# Body
{
   "youtube_url": "youtube-video-url"
}

# Monitor the local-transcrier instances as they transcribe the chunks and store them in the transcription-results topic. In a new terminal tab, run this command to monitor the daat inside chapter-results topic:

kubectl run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.41.0-kafka-3.7.0 --rm=true --restart=Never -n kafka -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 --topic chapter-results --from-beginning

# Optionally, you could also fire up the minikube dashboard to monitor the pods, services, topics etc.

minikube dashboard
```
