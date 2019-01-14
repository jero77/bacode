import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class provides the functionality for a clustering of the active domain of the relaxation attribute.
 * A clustering is a set of clusters, e.g. a set of subsets of values that occur in the column of the
 * relaxation attribute.
 * A cluster is identified by it's head element and for all the other values belonging to this cluster there
 * is a similarity value to the head element defined and this value is greater or equal to the in the
 * clustering-procedure defined similarity threshold.
 * The parameter type T is the domain of the relaxation attribute, e.g. a String or an Integer.
 */
public class Cluster<T> implements Serializable {

    /**
     * Head element of the cluster
     */
    private T head;

    /**
     * Subset of values of type T of the active domain belonging to this cluster (does not contain head!)
     */
    private Set<T> adom;


//##################### Constructors ######################

    public Cluster(T head, HashSet<T> adom) {
        this.head = head;
        this.adom = adom;
    }


    public Cluster(T head) {
        this.head = head;
        adom = new HashSet<T>();
    }

//##################### Getter & Setter ######################


    /**
     * Gets the head element of this cluster
     * @return Head element
     */
    public T getHead() {
        return head;
    }

    /**
     * Sets a new head element for this cluster
     * @param head
     */
    public void setHead(T head) {
        this.head = head;
    }


//##################### Modify adom-List (Getter/Setter) ######################

    public Set<T> getAdom() {
        return adom;
    }


//##################### Overwritten Methods #########################

    /**
     * Simple toString method
     * @return String representation of the cluster
     */
    @Override
    public String toString() {
        String s =  "Head of the cluster: " + this.head;
        for (Object t : this.adom)
            s += "\n\tTerm: " + t;
        return s;
    }


    /**
     * Compares a cluster to another object. Two clusters are equal if they are identified by the same head element.
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Cluster))
            return false;

        // Compare head elements
        Cluster other = (Cluster) obj;
        return this.head.equals(other.head);
    }
}
