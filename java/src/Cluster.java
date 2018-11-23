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
 * clustering-procedure ({@link MyAffinityFunction#clustering(List, double)} defined similarity threshold.
 * The parameter type T is the domain of the relaxation attribute, e.g. a String or an Integer.
 */
public class Cluster<T> {

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


    public boolean addAdomValue(T value) {
        return adom.add(value);
    }


    public boolean removeAdomValue(Collection<T> remove) {
        return adom.removeAll(remove);
    }
}
