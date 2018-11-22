import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * This is a compound key containing a person's id and the string for the disease's name.
 * Used to cache an instance of the Ill-class {@link Ill}
 */
public class IllKey {

    /**
     * The ID of a person. PersonID will be used for affinity collocation of the Ill-Cache. This will cause
     * an Info-object to be stored at the same partition together with the corresponding Ill-object.
     * {@link AffinityKeyMapped}
     */
    @AffinityKeyMapped
    private int personID;

    /**
     * The disease this person has/had.
     */
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

    public int getPersonID() {
        return personID;
    }

    public void setPersonID(int personID) {
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
