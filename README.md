<div align="center">
<h1 align="center">ChapGen</h1>
<p align="center">
<strong>AI-powered YouTube chapter generation, built on a fully cloud-native, event-driven architecture.</strong>
<br />
<br />
</p>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Kubernetes-f3f3f3?style=flat&logo=kubernetes&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/MinIO-61DAFB?style=flat&logo=minio&logoColor=%23000000&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Kafka-20BEFF?style=flat&logo=apachekafka&logoColor=%23fffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Gemini-8E75B2?style=flat&logo=googlegemini&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/HuggingFace-040404?style=flat&logo=huggingface&logoColor=%23FFD21E&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/GitHub Actions-2088FF?style=flat&logo=githubactions&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Nginx-009639?style=flat&logo=nginx&logoColor=%23ffffff&logoSize=auto"></span>
<span style="margin-top: 10px; width: 4rem; margin-right: 0.5rem;"><img alt="Static Badge" src="https://img.shields.io/badge/Google Cloud-4285F4?style=flat&logo=googlecloud&logoColor=%23ffffff&logoSize=auto"></span>
</br>
</div>

-----

## What's This All About?

Let's be real, manually adding chapters to YouTube videos is a drag. While YouTube Studio has a feature for this, it's often too generic and lacks fine-grained control. ChapGen is a tool that solves this problem by using AI to intelligently analyze a video's transcript and generate meaningful, semantic chapters.

But this project is more than just a cool tool. It's a showcase of a complete, production-grade, cloud-native system built from the ground up.

*(Add a UI GIF here\!)*

-----

## Architecture Deep Dive: How the Magic Happens

At its heart, ChapGen is an asynchronous, event-driven system built on a microservices architecture. This isn't your standard monolith; every component is decoupled and communicates through a central message bus (Kafka). This makes the system resilient, scalable, and a ton of fun to build (ironically of course 🤠).

Here's a bird's-eye view of the data flow:

<div align="center">
<img src="https://github.com/rkhatta1/ChapterGen/images/chapgen-arch.svg"> 
</div>

#### The Players:

  * **Frontend**: A snappy UI built with **React** and **Vite**, allowing users to authenticate with their Google account and kick off the process. Check that repo out [here.](https://github.com/rkhatta1/ChapterGenFrontend)
  * **Ingestion Service**: The front door to our backend. It takes a YouTube URL, uses `yt-dlp` to grab the audio, chunks it into manageable pieces, and fires off messages to Kafka for each chunk.
  * **Transcription Bridge**: This service acts as the mission control for our transcription tasks. It consumes chunk messages from Kafka and triggers our serverless GPU worker for each one.
  * **Serverless GPU Transcriber (The Star Player)**: This isn't an always-on, expensive VM. It's a **Google Cloud Run Job** with a GPU attached. It spins up in seconds when a job arrives, transcribes the audio using **OpenAI's Whisper** model, sends the results back, and then **scales down to zero**, costing absolutely nothing when it's idle.
  * **Chapter Generation Service**: The brains of the operation. It consumes the completed transcriptions, intelligently formats a prompt, and uses the **Google Gemini API** to generate the final chapter list.
  * **Database Service**: A simple FastAPI service that provides a REST API for managing user and job data in our **PostgreSQL** database.
  * **Frontend Bridge**: Manages the persistent **WebSocket** connection with the client, pushing the final, generated chapters back to the UI in real-time.

-----

## Deployment & Infrastructure

This project runs on a professional-grade cloud setup, configured declaratively using Kubernetes manifests.

  * **Cloud Provider**: **Google Cloud Platform (GCP)**.
  * **Orchestration**: A lightweight **K3s** cluster runs on a single **Google Compute Engine** VM, managing all the core backend services.
  * **Networking**: All traffic is routed through an **Nginx Ingress Controller**. SSL is handled automatically by **`cert-manager`**, which provisions free, trusted certificates from **Let's Encrypt**.
  * **Serverless GPU**: The transcription workload is deployed as a **Google Cloud Run Job** with an attached GPU, providing a scalable and extremely cost-effective solution.

-----

## Future Work

  * **CI/CD Pipeline**: Implement the full CI/CD pipeline with GitHub Actions to automate the building and deployment of services on every push to `main`.
  * **Frontend UI/UX**: Clean up and enhance the user interface.
  * **Monitoring**: Integrate monitoring tools to get better visibility into the health and performance of the services.