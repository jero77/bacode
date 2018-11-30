import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class InfoKey {

    /**
     * ID of a person.
     */
    @QuerySqlField (index = true)
    private int id;


    /**
     * This field is used to collocate a person's information to the information of its diseases. It denotes one of the
     * partition numbers the person will be mapped to, which means that a person's information will be found on each
     * node that stores one (or more) disease(s) this person has. Hence, no distributed joins are needed.
     * For SQL this field is not visible, s.t. the person's information is mapped equally (in the sense of
     * not distinguishable for SQL) to all the nodes it has to be present on, which results in a replication and a
     * derived fragmentation, allowing Joins on Ill- and Info-Table (not distributed, but collocated)
     */
    private int affinityPartition;



//#################### Constructors ####################

    /**
     * Constructor for a InfoKey
     * @param id ID of a person
     */
    public InfoKey(int id, int affinityPartition) {
        this.id = id;
        this.affinityPartition = affinityPartition;
    }

//#################### Getter & Setter ####################

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAffinityPartition() {
        return affinityPartition;
    }

    public void setAffinityPartition(int affinityPartition) {
        this.affinityPartition = affinityPartition;
    }
}
