package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import tn.utm.kafka.models.EvenementPOS;

import java.util.*;

/**
 * Simule une caisse qui génère des événements aléatoires
 */
public class SimulateurCaisse {

    private static final String[] VILLES = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabes"};
    private static final String[] PRODUITS = {
            "Pain", "Lait", "Fromage", "Poulet", "Tomates",
            "Pommes", "Riz", "Pâtes", "Huile", "Sucre"
    };

    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║   🏪 SIMULATEUR DE CAISSE - DÉMARRÉ   ║");
        System.out.println("╔════════════════════════════════════════╝\n");

        // Configuration du Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {

            int compteur = 0;

            // Boucle infinie de génération d'événements
            while (true) {
                // Générer un événement aléatoire
                EvenementPOS event = genererEvenement();

                // Convertir en JSON
                String json = mapper.writeValueAsString(event);

                // Clé = ville (pour partitionnement)
                String key = event.getVille();

                // Créer le record
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("pos-events", key, json);

                // Envoyer
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("❌ Erreur : " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "✅ [%s] %s → Partition %d, Offset %d\n",
                                event.getType(),
                                event.getVille(),
                                metadata.partition(),
                                metadata.offset()
                        );
                    }
                });

                compteur++;

                // Afficher un résumé tous les 10 événements
                if (compteur % 10 == 0) {
                    System.out.println("\n📊 " + compteur + " événements envoyés\n");
                }

                // Pause aléatoire entre 100ms et 500ms
                Thread.sleep(100 + random.nextInt(400));
            }

        } catch (Exception e) {
            System.err.println("❌ Erreur fatale : " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Génère un événement POS aléatoire
     */
    private static EvenementPOS genererEvenement() {
        String ville = VILLES[random.nextInt(VILLES.length)];
        String idCaisse = "CAISSE-" + ville.toUpperCase() + "-" + (1 + random.nextInt(5));

        // Probabilités : 70% VENTE, 20% OUVERTURE, 10% RETOUR
        double rand = random.nextDouble();
        String type;
        Double montant;
        List<String> produits;

        if (rand < 0.70) {
            // VENTE
            type = "VENTE";
            montant = 5.0 + random.nextDouble() * 495.0; // 5 à 500 DT
            produits = genererProduits(1 + random.nextInt(5));

        } else if (rand < 0.90) {
            // OUVERTURE
            type = "OUVERTURE";
            montant = 0.0;
            produits = new ArrayList<>();

        } else {
            // RETOUR
            type = "RETOUR";
            montant = 10.0 + random.nextDouble() * 290.0; // 10 à 300 DT
            produits = genererProduits(1 + random.nextInt(3));
        }

        return new EvenementPOS(type, idCaisse, ville, montant, produits);
    }

    /**
     * Génère une liste aléatoire de produits
     */
    private static List<String> genererProduits(int nombre) {
        List<String> liste = new ArrayList<>();
        for (int i = 0; i < nombre; i++) {
            liste.add(PRODUITS[random.nextInt(PRODUITS.length)]);
        }
        return liste;
    }
}