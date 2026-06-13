# University Data Platform - MVP Challenge

## 📌 Présentation du Projet
Ce projet vise à construire une plateforme de données universitaire complète en 3 semaines. L'objectif est de collecter des données provenant de plusieurs sources (API, Web, Fichiers), de les stocker, de les transformer avec Spark et de les rendre accessibles via un dashboard et un moteur de recherche.

## 🏗️ Architecture du Système
![Architecture du Projet](./docs/architecture.png)
*Note : Veuillez vous référer au schéma dans le dossier /docs pour le détail des flux.*

## 👥 Équipe et Rôles
- **[TON NOM] (Délégué)** : Qualité, Logs & Documentation (Membre 6)
- **Membre 1** : Lead DevOps & Infra (Docker, MinIO, ES)
- **Membre 2** : Ingestion API (OpenAlex)
- **Membre 3** : Ingestion Web (Scraping institutionnel)
- **Membre 4** : Ingestion Fichiers (PDF/CSV)
- **Membre 5** : Lead Spark Processing (ETL & Hudi)

## 🛠️ Guide d'Installation (Docker)

### Pré-requis
- Docker et Docker Compose installés.
- Python 3.9+ (pour les scripts d'ingestion).

### Lancement de l'infrastructure
1. Clonez le dépôt :
   ```bash
   git clone https://github.com/merycmo/university-data-platform.git
   cd university-data-platform