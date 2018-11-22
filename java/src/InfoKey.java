import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class InfoKey {

    /**
     * The ID of a person. PersonID will be used for affinity collocation of the Ill-Cache. This will cause
     * an Info-object to be stored at the same partition together with the corresponding Ill-object.
     * {@link AffinityKeyMapped}
     */
    @AffinityKeyMapped
    private int personID;

//#################### Constructors ####################

    public InfoKey(int personID) {
        this.personID = personID;
    }

//#################### Getter & Setter ####################

    public int getPersonID() {
        return personID;
    }

    public void setPersonID(int personID) {
        this.personID = personID;
    }


//##################### toString #########################

    @Override
    public String toString() {
        String s = "PersonID: " + personID;
        return s;
    }

}
