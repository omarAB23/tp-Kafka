# TP Apache Kafka — Mini-projet Pipeline Temps Réel

## 📌 Prérequis

* Java JDK 17+
* Apache Kafka (version 3.x ou 4.x)
* Maven 3.8+
* IntelliJ IDEA (ou autre IDE)

---

## 📁 Structure du projet

```
tp-kafka-java/
 ├── src/main/java/tn/utm/kafka/
 │   ├── SimulateurCaisse.java
 │   ├── ChiffreAffairesParVille.java
 │   ├── DetecteurAnomalies.java
 ├── pom.xml
```

---

## ⚙️ Démarrage de Kafka

### 1. Définir les variables d’environnement (Windows PowerShell)

```
$env:KAFKA_HOME="C:\kafka"
$env:Path="$env:KAFKA_HOME\bin\windows;$env:Path"
```

---

### 2. Créer les dossiers

```
mkdir C:\kafka-data\logs
```

---

### 3. Générer un Cluster ID

```
kafka-storage.bat random-uuid
```

Copier l’ID généré.

---

### 4. Formater le stockage

```
kafka-storage.bat format --config C:\kafka-data\server.properties --cluster-id VOTRE_ID
```

---

### 5. Démarrer Kafka

```
kafka-server-start.bat C:\kafka-data\server.properties
```

---

## 📡 Création des topics

```
kafka-topics.bat --create --topic pos-events --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

kafka-topics.bat --create --topic alertes-retours --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

---

## ▶️ Lancement des applications

### 1. Lancer le consommateur CA

```
ChiffreAffairesParVille
```

---

### 2. Lancer le détecteur d’anomalies

```
DetecteurAnomalies
```

---

### 3. Lancer le simulateur (producer)

```
SimulateurCaisse
```

---

## 📊 Résultat attendu

* Génération continue d’événements
* Calcul du chiffre d’affaires par ville
* Détection des retours > 200 DT

---

## 🧪 Tests réalisés

* Exécution avec plusieurs simulateurs
* Exécution avec plusieurs consommateurs
* Test de rebalance lors de l’ajout/suppression de consumers

---

## 👨‍💻 Auteur

Nom : Omar Abdelkader
TP Kafka — Déploiement mono-machine
