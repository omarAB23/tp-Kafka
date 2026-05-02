package tn.utm.kafka.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;

/**
 * Représente un événement POS (Point Of Sale - Caisse)
 */
public class EvenementPOS {

    @JsonProperty("type")
    private String type; // VENTE, RETOUR, OUVERTURE

    @JsonProperty("idCaisse")
    private String idCaisse;

    @JsonProperty("ville")
    private String ville;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("montant")
    private Double montant;

    @JsonProperty("produits")
    private List<String> produits;

    // Constructeur vide (requis par Jackson)
    public EvenementPOS() {
    }

    // Constructeur complet
    public EvenementPOS(String type, String idCaisse, String ville,
                        Double montant, List<String> produits) {
        this.type = type;
        this.idCaisse = idCaisse;
        this.ville = ville;
        this.timestamp = Instant.now().toString();
        this.montant = montant;
        this.produits = produits;
    }

    // Getters et Setters
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIdCaisse() {
        return idCaisse;
    }

    public void setIdCaisse(String idCaisse) {
        this.idCaisse = idCaisse;
    }

    public String getVille() {
        return ville;
    }

    public void setVille(String ville) {
        this.ville = ville;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getMontant() {
        return montant;
    }

    public void setMontant(Double montant) {
        this.montant = montant;
    }

    public List<String> getProduits() {
        return produits;
    }

    public void setProduits(List<String> produits) {
        this.produits = produits;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s - %s - %.2f DT - %s",
                type, ville, idCaisse, montant != null ? montant : 0.0,
                produits != null ? produits : "[]");
    }
}