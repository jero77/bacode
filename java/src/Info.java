import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Info implements Serializable {

    /**
     * This key is used for affinity mapping to achieve a derived fragmentation of the {@link Ill}-Table.
     * It is necessary, because in this way the affinity function can distiguish between {@link Ill}- and
     * {@link Info}-Objects properly.
     */
    @AffinityKeyMapped
    private InfoKey key;

    @QuerySqlField
    private String name;

    @QuerySqlField
    private String address;



//#################### Constructors ####################


    public Info(InfoKey key) {
        this.key = key;
        this.name = "Name of " + key.getId();
        this.address = "Address of " + key.getId();
    }

    public Info(InfoKey key, String name, String address) {
        this.key = key;
        this.name = name;
        this.address = address;
    }

//#################### Getter & Setter ####################


    public InfoKey getKey() {
        return key;
    }

    public void setKey(InfoKey key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

//##################### toString #########################

    @Override
    public String toString() {
        String s = "PersonID: " + key.getId() + ", Name: " + name + ", Address: " + address;
        return s;
    }

}
