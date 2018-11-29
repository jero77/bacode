import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Info implements Serializable {

    /**
     * The ID (key) of a person. This key will be used for affinity collocation of the Ill-Cache and this will cause
     * an Info-object to be stored at the same partition together with the corresponding Ill-object.
    */
    @AffinityKeyMapped
    @QuerySqlField (index = true)
    private Integer id;

    @QuerySqlField
    private String name;

    @QuerySqlField
    private String address;



//#################### Constructors ####################

    public Info(int id, String name, String address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }


    public Info(int id) {
        this.id = id;
        this.name = "Name of " + id;
        this.address = "Address of " + id;
    }


//#################### Getter & Setter ####################


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
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
        String s = "PersonID: " + id + ", Name: " + name + ", Address: " + address;
        return s;
    }

}
