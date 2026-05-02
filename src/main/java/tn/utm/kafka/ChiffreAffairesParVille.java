package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import tn.utm.kafka.models.EvenementPOS;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Calcule le chiffre d'affaires par ville en temps réel
 */
public class ChiffreAffairesParVille {

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // Map pour stocker le CA par ville
    private static final Map<String, Double> caParVille = new HashMap<>();
    private static long dernierAffichage = System.currentTimeMillis();

    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║   💰 CALCUL CHIFFRE D'AFFAIRES        ║");
        System.out.println("╔════════════════════════════════════════╝\n");

        // Configuration du Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ca-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // S'abonner au topic
            consumer.subscribe(Collections.singletonList("pos-events"));

            System.out.println("👂 En attente d'événements...\n");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Désérialiser le JSON
                        EvenementPOS event = mapper.readValue(record.value(), EvenementPOS.class);

                        // Ne traiter que VENTE et RETOUR
                        if ("VENTE".equals(event.getType())) {
                            // Ajouter au CA
                            caParVille.merge(event.getVille(), event.getMontant(), Double::sum);

                        } else if ("RETOUR".equals(event.getType())) {
                            // Soustraire du CA
                            caParVille.merge(event.getVille(), -event.getMontant(), Double::sum);
                        }

                    } catch (Exception e) {
                        System.err.println("❌ Erreur de parsing : " + e.getMessage());
                    }
                }

                // Commit après traitement
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                // Afficher le CA toutes les 5 secondes
                long maintenant = System.currentTimeMillis();
                if (maintenant - dernierAffichage >= 5000) {
                    afficherCA();
                    dernierAffichage = maintenant;
                }
            }

        } catch (Exception e) {
            System.err.println("❌ Erreur fatale : " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Affiche le chiffre d'affaires par ville
     */
    private static void afficherCA() {
        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║   📊 CHIFFRE D'AFFAIRES PAR VILLE     ║");
        System.out.println("╚════════════════════════════════════════╝");

        if (caParVille.isEmpty()) {
            System.out.println("   Aucune donnée pour le moment...");
        } else {
            double total = 0.0;
            for (Map.Entry<String, Double> entry : caParVille.entrySet()) {
                System.out.printf("   %-15s : %,.2f DT\n", entry.getKey(), entry.getValue());
                total += entry.getValue();
            }
            System.out.println("   ─────────────────────────────────");
            System.out.printf("   %-15s : %,.2f DT\n", "TOTAL", total);
        }
        System.out.println("╚════════════════════════════════════════╝\n");
    }
}