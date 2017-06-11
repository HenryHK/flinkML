package ml;

/**
 * Created by lhan on 17-6-1.
 */
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
//import com.google.protobuf.joi

public class ItemSet implements Serializable, Comparable {

    public ArrayList<Integer> items;
    private int numberOfTransactions;

    // empty ItemSet
    public ItemSet() {
        this.items = new ArrayList<>();
        this.numberOfTransactions = 0;
    }

    // ItemSet from an item
    public ItemSet(Integer item) {
        this.items = new ArrayList<>();
        this.items.add(item);
        this.numberOfTransactions = 1;
    }

    // ItemSet from list of items
    public ItemSet(ArrayList<Integer> itemList) {
        this.items = itemList;
    }

    public void setNumberOfTransactions(int numberOfTransactions) {
        this.numberOfTransactions = numberOfTransactions;
    }

    public int getNumberOfTransactions() {
        return numberOfTransactions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        ItemSet rhs = (ItemSet) obj;
        return new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(items, rhs.items)
                .append(numberOfTransactions, rhs.numberOfTransactions)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31)
                .append(items)
                .append(numberOfTransactions)
                .toHashCode();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(numberOfTransactions);
        Collections.sort(items);
        for(Integer i : items){
            sb.append("\t"+i);
        }
        return sb.toString();


    }


    @Override
    public int compareTo(Object o) {
        ItemSet i = (ItemSet)o;
        if(i.items.containsAll(this.items)&&i.items.size()==this.items.size()){
            return 0;
        }else{
            return 1;
        }
    }
}

