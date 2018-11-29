import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * This is a compound key containing a person's id and the string for the disease's name.
 * Used to cache an instance of the Ill-class {@link Ill}
 */
public class IllKey implements Serializable {

    /**
     * The ID of a person.
     */
    @QuerySqlField (index = true)
    private Integer personID;

    /**
     * The disease this person has/had. This term will be used for the clustering-based fragmentation.
     * Affinity collocation based on this attribute (relaxation attribute) is used to obtain a mapping
     * from a disease term to a partition (to a node)
     * {@link AffinityKeyMapped}
     * {@link MyAffinityFunction}
     */
    @AffinityKeyMapped
    @QuerySqlField
    private String disease;


//##################### Constructor #########################

    /**
     * Constructor for an IllKey-object.
     * {@link Ill}
     */
    public IllKey(int personID, String disease) {
        this.personID = personID;
        this.disease = disease;
    }


//##################### Getter & Setter #########################

    public Integer getPersonID() {
        return personID;
    }

    public void setPersonID(Integer personID) {
        this.personID = personID;
    }

    public String getDisease() {
        return disease;
    }

    public void setDisease(String disease) {
        this.disease = disease;
    }


//##################### toString #########################

    @Override
    public String toString() {
        String s = "PersonID: " + personID + ", Disease: " + disease;
        return s;
    }

}
