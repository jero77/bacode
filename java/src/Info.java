import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class Info {

    // Person ID will be used for affinity with the Ill-Cache
    // --> store tuple describing a person's name and address together with tuples describing his illnesses
    // @AffinityKeyMapped
    private InfoKey key;

    private String name;
    private String address;



//#################### Constructors ####################

    public Info(InfoKey key, String name, String address) {
        this.key = key;
        this.name = name;
        this.address = address;
    }


    public Info(InfoKey key) {
        this.key = key;
        this.name = "Name of " + key;
        this.address = "Address of " + key;
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
        String s = "PersonID: " + key.getPersonID() + ", Name: " + name + ", Address: " + address;
        return s;
    }

}
