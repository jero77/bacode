public class Ill {

    /**
     * Key which will be used to cache an Ill-Object {@link IllKey}.
      */
    private IllKey key;

    /**
     * This is the id of the descriptor in MeSH
      */
    private String meshID;;

    // Some other attributes ....


//##################### Constructor #########################

    public Ill(IllKey key, String meshID) {
        this.key = key;
        this.meshID = meshID;
    }


//##################### Getter & Setter #########################

    public IllKey getKey() {
        return key;
    }

    public void setKey(IllKey key) {
        this.key = key;
    }

    public String getMeshID() {
        return meshID;
    }

    public void setMeshID(String meshID) {
        this.meshID = meshID;
    }


//##################### toString #########################

    @Override
    public String toString() {
        String s = "PersonID: " + key.getPersonID() + ", Disease(MeSH-ID): " + key.getDisease() + "(" + meshID + ")";
        return s;
    }
}
