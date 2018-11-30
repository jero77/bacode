import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class InfoKey {

    /**
     * ID of a person. This attribute is used to collocate information about a person (e.g. a tuple/row/key-value-pair)
     * to the same partition as the person's disease information.
     */
    @QuerySqlField (index = true)
    private int id;

//#################### Constructors ####################

    /**
     * Constructor for a InfoKey
     * @param id ID of a person
     */
    public InfoKey(int id) {
        this.id = id;
    }

//#################### Getter & Setter ####################

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
