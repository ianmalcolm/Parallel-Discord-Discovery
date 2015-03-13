package SAXFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import edu.hawaii.jmotif.sax.datastructures.DiscordRecord;

/**
 * The discord records collection.
 *
 * @author Pavel Senin
 *
 */
public class DiscordRecords implements Iterable<DiscordRecord> {

    /**
     * Storage container.
     */
    private final LinkedList<DiscordRecord> discords;
    private final int k;
    private final int windowSize;

    /**
     * Constructor.
     */
    public DiscordRecords(int reportnum) {
        discords = new LinkedList<DiscordRecord>();
        k = reportnum;
        windowSize = 0;
    }

    public DiscordRecords(int reportnum, int window) {
        discords = new LinkedList<DiscordRecord>();
        k = reportnum;
        windowSize = window;
    }

    public DiscordRecords() {
        discords = new LinkedList<DiscordRecord>();
        k = Integer.MAX_VALUE;
        windowSize = 0;
    }

    private boolean isOverlapped(DiscordRecord d1, DiscordRecord d2) {
        assert windowSize > 0;
        if (Math.abs(d1.getPosition() - d2.getPosition()) < windowSize) {
            return true;
        } else {
            return false;
        }
    }

    // i starts from 0
//    public DiscordRecord get(int i) {
//        Collections.sort(discords);
//        return discords.get(i);
//    }
    /**
     * Add a new discord to the list. Here is a trick. This method will also
     * check if the current distance is less than best so far (best in the
     * table). If so - there is no need to continue that inner loop - the MAGIC
     * optimization.
     *
     * @param discord The discord instance to add.
     * @return if the discord got added.
     */
    public boolean add(DiscordRecord discord) {

        // System.out.println(" + discord record " + discord);
        // more complicated - need to check if it will fit in there
        // DiscordRecord last = discords.get(discords.size() - 1);
        if (!discords.contains(discord)) {
            boolean overlap = false;
            DiscordRecord toberemove = null;
            for (DiscordRecord d : discords) {
                if (isOverlapped(d, discord)) {
                    overlap = true;
                    if (d.getDistance() < discord.getDistance()) {
                        discords.add(discord);
                        toberemove = d;
                        break;
                    }
                }
            }
            if (toberemove != null) {
                discords.remove(toberemove);
            }
            if (!overlap) {
                discords.add(discord);
            }
            Collections.sort(discords);
            if (discords.size() > k) {
                discords.remove(0);
            }
            return true;
        } else {
            return false;
        }
    }

    public void add(DiscordRecords inputDiscords) {
        for (DiscordRecord dr : inputDiscords) {
            add(dr);
        }
    }

    /**
     * Returns the number of the top hits.
     *
     * @param num The number of instances to return. If the number larger than
     * the storage size - returns the storage as is.
     * @return the top discord hits.
     */
    public List<DiscordRecord> getTopHits(Integer num) {
        Collections.sort(discords);
        if (num >= this.discords.size()) {
            return this.discords;
        }
        List<DiscordRecord> res = this.discords.subList(this.discords.size() - num,
                this.discords.size());
        Collections.reverse(res);
        return res;
    }

    /**
     * Get the minimal distance found among all instances in the collection.
     *
     * @return The minimal distance found among all instances in the collection.
     */
//    public double getMinDistance() {
//        if (this.discords.size() > 0) {
//            Collections.sort(discords);
//            return discords.get(0).getDistance();
//        }
//        return -1;
//    }
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(1024);
        for (DiscordRecord r : discords) {
            sb.append("discord \"" + r.getPayload() + "\", at " + r.getPosition()
                    + " distance to closest neighbor: " + r.getDistance() + "\n");
        }
        return sb.toString();
    }

    @Override
    public Iterator<DiscordRecord> iterator() {
        return this.discords.iterator();
    }

    public int getSize() {
        return this.discords.size();
    }

    public double getWorstDistance() {
        if (this.discords.isEmpty()) {
            return 0D;
        }
        Collections.sort(discords);
        return discords.get(0).getDistance();
    }

    // return the Kth biggest distnace
    public double getKthDistance() {

        if (k < Integer.MAX_VALUE) {
            if (getSize() < k) {
                return Double.MIN_VALUE;
            } else {
                Collections.sort(discords, Collections.reverseOrder());
                return discords.get(k - 1).getDistance();
            }
        } else {
            return Double.MIN_VALUE;
        }
    }

    // return the Kth biggest distance
    public double getKthDistance(int i) {
        if (getSize() < i) {
            return Double.MIN_VALUE;
        } else {
            Collections.sort(discords, Collections.reverseOrder());
            return discords.get(i).getDistance();
        }
    }

    // return the Kth biggest distance
    public int getKthPosition(int i) {
        if (getSize() < i) {
            return -1;
        } else {
            Collections.sort(discords, Collections.reverseOrder());
            return discords.get(i).getPosition();
        }
    }

    public String getKthPayload(int i) {
        if (getSize() < i) {
            return "";
        } else {
            Collections.sort(discords, Collections.reverseOrder());
            return discords.get(i).getPayload();
        }
    }

    // set the distance of the top ith discord
    // i starts from 0
    public int setDistance(int i, double dist) {
        if (i < 0 || i >= getSize()) {
            return -1;
        }
        Collections.sort(discords, Collections.reverseOrder());
        discords.get(i).setDistance(dist);
        return 0;
    }

    public String toCoordinates() {
        StringBuffer sb = new StringBuffer();
        for (DiscordRecord r : discords) {
            sb.append(r.getPosition() + ",");
        }
        return sb.delete(sb.length() - 1, sb.length()).toString();
    }

    public String toPayloads() {
        StringBuffer sb = new StringBuffer();
        for (DiscordRecord r : discords) {
            sb.append("\"" + r.getPayload() + "\",");
        }
        return sb.delete(sb.length() - 1, sb.length()).toString();
    }

    public String toDistances() {
        NumberFormat nf = new DecimalFormat("##0.####");
        StringBuffer sb = new StringBuffer();
        for (DiscordRecord r : discords) {
            sb.append(nf.format(r.getDistance()) + ",");
        }
        return sb.delete(sb.length() - 1, sb.length()).toString();
    }
}
