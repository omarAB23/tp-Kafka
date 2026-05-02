package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import tn.utm.kafka.models.EvenementPOS;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Détecte les retours anormaux (> 200 DT) et envoie des alertes
 */
public class DetecteurAnomalies {

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private static final double SEUIL_ALERTE = 200.0;

    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║   🚨 DÉTECTEUR D'ANOMALIES            ║");
        System.out.println("╔════════════════════════════════════════╝\n");
        System.out.println("⚠️  Seuil d'alerte : " + SEUIL_ALERTE + " DT\n");

        // Configuration Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "alerte-1");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Configuration Producer (pour les alertes)
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             Producer<String, String> producer = new KafkaProducer<>(producerProps)) {

            // S'abonner au topic
            consumer.subscribe(Collections.singletonList("pos-events"));

            System.out.println("👂 Surveillance en cours...\n");

            int compteurAlertes = 0;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Désérialiser
                        EvenementPOS event = mapper.readValue(record.value(), EvenementPOS.class);

                        // Vérifier si c'est un RETOUR anormal
                        if ("RETOUR".equals(event.getType()) &&
                                event.getMontant() != null &&
                                event.getMontant() > SEUIL_ALERTE) {

                            compteurAlertes++;

                            // Créer le message d'alerte
                            String alerte = String.format(
                                    "🚨 ALERTE #%d - RETOUR ANORMAL détecté :\n" +
                                            "   Ville      : %s\n" +
                                            "   Caisse     : %s\n" +
                                            "   Montant    : %.2f DT\n" +
                                            "   Timestamp  : %s\n",
                                    compteurAlertes,
                                    event.getVille(),
                                    event.getIdCaisse(),
                                    event.getMontant(),
                                    event.getTimestamp()
                            );

                            System.out.println(alerte);

                            // Envoyer l'alerte dans un topic dédié
                            ProducerRecord<String, String> alerteRecord =
                                    new ProducerRecord<>("alertes-retours",
                                            event.getVille(),
                                            record.value());

                            producer.send(alerteRecord, (metadata, exception) -> {
                                if (exception == null) {
                                    System.out.println("   ✅ Alerte envoyée au topic 'alertes-retours'\n");
                                }
                            });
                        }

                    } catch (Exception e) {
                        System.err.println("❌ Erreur : " + e.getMessage());
                    }
                }

                // Commit
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Erreur fatale : " + e.getMessage());
            e.printStackTrace();
        }
    }
}