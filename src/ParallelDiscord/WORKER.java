/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ParallelDiscord;

import SAXFactory.SAXFactory;
import SAXFactory.DiscordRecords;
import PreciseDiscord.PreciseDiscord;
import Distance.ED;
import edu.hawaii.jmotif.sax.DistanceEntry;
import edu.hawaii.jmotif.sax.LargeWindowAlgorithm;
import edu.hawaii.jmotif.sax.SlidingWindowMarkerAlgorithm;
import edu.hawaii.jmotif.sax.alphabet.NormalAlphabet;
import edu.hawaii.jmotif.sax.datastructures.DiscordRecord;
import edu.hawaii.jmotif.sax.trie.SAXTrie;
import edu.hawaii.jmotif.sax.trie.SAXTrieHitEntry;
import edu.hawaii.jmotif.sax.trie.TrieException;
import edu.hawaii.jmotif.sax.trie.VisitRegistry;
import edu.hawaii.jmotif.timeseries.TSException;
import edu.hawaii.jmotif.timeseries.TSUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ian
 */
public class WORKER {

    private final int offset;
    private final double[] segment;
    private final int windowSize;
    private final int alphabetSize;
    private final SlidingWindowMarkerAlgorithm marker = new LargeWindowAlgorithm();
    private final DiscordRecords drs = new DiscordRecords();
    private final TreeMap<String, DistanceEntry> knownWordsAndTheirCurrentDistances = new TreeMap<>();
    private final TreeSet<String> completeWords = new TreeSet<>();
    private final NormalAlphabet normalA = new NormalAlphabet();
    private final VisitRegistry discordsVisitRegistry;
    private final SAXTrie trie;
    private final ArrayList<String> wordList;
    private static final Logger workerlogger = Logger.getLogger(WORKER.class.getName());
    private int ttDistFuncCnt = 0;
    private boolean EXHAUSTED = false;
    private final boolean PRECISE;
    private final double[] NNDists;
    private boolean ABANDONED = false;

    public WORKER(double[] series, int window, int alphabet, int start, boolean precise) throws TrieException, TSException {

        // initialize
        segment = series;
        windowSize = window;
        alphabetSize = alphabet;
        offset = start;
        PRECISE = precise;

        // build SAXTrie index
        trie = new SAXTrie(segment.length - windowSize + 1, alphabetSize);
        for (int i = 0; i < segment.length - windowSize + 1; i++) {
            double[] subSeries = SAXFactory.getSubSeries(series, i, i + windowSize);
            char[] saxVals = SAXFactory.getSaxVals(subSeries, windowSize, normalA.getCuts(alphabetSize));
            trie.put(String.valueOf(saxVals), i);
        }
        discordsVisitRegistry = new VisitRegistry(segment.length - windowSize + 1);
        wordList = getAllWords();

        NNDists = new double[segment.length - windowSize + 1];
        for (int i = 0; i < NNDists.length; i++) {
            NNDists[i] = Double.MAX_VALUE;
        }
    }

    public static void setLoggerLevel(Level level) {
        workerlogger.setLevel(level);
    }

    public int getSegmentSize() {
        return segment.length;
    }

    public boolean includes(int p) {
        if (p >= getOffset() && p <= getOffset() + segment.length - windowSize) {
            return true;
        } else {
            return false;
        }
    }

    public int getOffset() {
        return offset;
    }

    public int getEnd() {
        return offset + segment.length;
    }

    public DiscordRecords getDiscords() {
        return drs;
    }

    public double getWorstDistance() {
        return drs.getWorstDistance();
    }

    public ArrayList<Double> getAllDistance() {
        ArrayList<Double> dist = new ArrayList();
        for (DiscordRecord dr : drs) {
            dist.add(dr.getDistance());
        }
        return dist;
    }

    private int getGlobalOffset(int localOffset) {
        return localOffset + offset;
    }

    public DiscordRecord findNextDiscord() throws TSException, TrieException {

        if (EXHAUSTED || ABANDONED) {
            return null;
        }
        int[] distFuncCnt = new int[]{0};
        DiscordRecord bestDiscord;
        Date start = new Date();
//        if (PRECISE) {
        bestDiscord = PreciseDiscord.findNextDiscord(segment, windowSize, trie, discordsVisitRegistry, NNDists, distFuncCnt);
//        } else {
//            bestDiscord = SAXFactory.findBestDiscord(segment, windowSize, trie, completeWords, knownWordsAndTheirCurrentDistances, discordsVisitRegistry, marker, distFuncCnt);
//        }
        Date end = new Date();

        // if the discord is null we getting out of the search
        if (bestDiscord.getDistance() == 0.0D || bestDiscord.getPosition() == -1) {
            workerlogger.fine("breaking the outer search loop, discords found: " + drs.getSize()
                    + " last seen discord: " + bestDiscord.toString());
            EXHAUSTED = true;
            return null;
        }

        workerlogger.fine("WORKER at " + offset + " finds #" + drs.getSize() + " discord: " + bestDiscord.getPayload() + " at "
                + getGlobalOffset(bestDiscord.getPosition()) + ", distance " + bestDiscord.getDistance()
                + ", elapsed time: " + SAXFactory.timeToString(start.getTime(), end.getTime()) + ", #distfunc: " + distFuncCnt[0]);

        // collect the result
        //
        drs.add(bestDiscord);

        // and maintain data structures
        //
        marker.markVisited(discordsVisitRegistry, bestDiscord.getPosition(), windowSize);
        completeWords.add(String.valueOf(bestDiscord.getPayload()));

        ttDistFuncCnt = ttDistFuncCnt + distFuncCnt[0];

//        Instance d = new DenseInstance(2);
        return bestDiscord;

    }

    public void setAbandon() {
        ABANDONED = true;
        workerlogger.info("Worker at " + getOffset() + " abandoned after finding " + drs.getSize() + " discords");
    }

    public boolean isAbandoned() {
        return ABANDONED;
    }

    public double findApproxNNDist(double[] query) throws TSException, TrieException {
        assert query != null;
        char[] saxVals = SAXFactory.getSaxVals(query, windowSize, normalA.getCuts(alphabetSize));
        sortWordsBySimilar(saxVals);
        List<Integer> occurences = trie.getOccurences(wordList.get(0).toCharArray());
        double minDist = Double.MAX_VALUE;
        for (Integer occu : occurences) {
            double[] seriesA = SAXFactory.getSubSeries(segment, occu, occu + windowSize);
            double curDist = ED.distance(seriesA, query);
            if (curDist < minDist) {
                minDist = curDist;
            }
        }

//        double minDist = ED.nndist(query, segment);
        return minDist;
    }

    public double findNNDist(double[] query) throws TSException, TrieException {
        double bsf = this.findApproxNNDist(query);
        if (PRECISE == true) {
            ArrayList<ELE> q = new ArrayList();
            ArrayList<Double> t = new ArrayList();

            double bsf2 = bsf * bsf;
            for (int i = 0; i < query.length; i++) {
                q.add(new ELE(i, query[i]));
            }
            Collections.sort(q, ELE.dataDescending);
            for (int i = 0; i < segment.length - windowSize + 2; i++) {
                t.add(segment[i]);
                if (t.size() < windowSize) {
                    continue;
                }
                double dist2 = distance2(q, t, bsf2);
                if (dist2 < bsf2) {
                    bsf2 = dist2;
                }
                t.remove(0);
            }
            return Math.sqrt(bsf2);
        } else {
            return bsf;
        }
    }

    private static double distance2(ArrayList<ELE> q, ArrayList<Double> t, double bsf2) {
        double sum = 0;
        for (int i = 0; i < q.size() && sum < bsf2; i++) {
            double diff = (t.get(q.get(i).index) - q.get(i).data);
            sum += diff * diff;
        }
        return sum;
    }

    // The most similar word appears at the beginning of the wordList
    private void sortWordsBySimilar(char[] inputword) throws TSException {
        double[][] distMatrix = normalA.getDistanceMatrix(alphabetSize);
        ArrayList<Double> distList = new ArrayList();
        for (String word : wordList) {
            distList.add(SAXFactory.saxMinDist(word.toCharArray(), inputword, distMatrix));
        }
        for (int i = 0; i < distList.size() - 1; i++) {
            for (int j = 0; j < distList.size() - i - 1; j++) {
                if (distList.get(j) > distList.get(j + 1)) {
                    String oswap = wordList.get(j);
                    double dswap = distList.get(j);
                    wordList.set(j, wordList.get(j + 1));
                    distList.set(j, distList.get(j + 1));
                    wordList.set(j + 1, oswap);
                    distList.set(j + 1, dswap);
                }
            }
        }
    }

    private ArrayList<String> getAllWords() {
        ArrayList<SAXTrieHitEntry> freq = trie.getFrequencies();
        ArrayList<String> words = new ArrayList();
        for (SAXTrieHitEntry a : freq) {
            String word = new String(a.getStr());
            if (words.contains(word)) {
                continue;
            }
            words.add(word);
        }
        return words;
    }

    public int refineDiscord(int index, double dist) {
        for (int i = 0; i < drs.getSize(); i++) {
            int kthposition = drs.getKthPosition(i) + offset;
            double kthdistance = drs.getKthDistance(i);
            String kthpayload = drs.getKthPayload(i);
            if (kthposition == index) {
                if (kthdistance > dist) {
                    workerlogger.fine("WORKER at " + offset + " refines dist of #" + i + "/" + drs.getSize()
                            + " discord " + kthpayload + " at position " + kthposition + " from " + kthdistance + " to " + dist);
                    drs.setDistance(i, dist);
                    return 0; // discord refined
                }
                return 2; // found discord, but no refinement
            }
        }
        return 1; // discord not found
    }

}

class ELE implements Comparable<ELE> {

    final double data;
    final int index;

    ELE(int i, double d) {
        index = i;
        data = d;
    }

    @Override
    public int compareTo(ELE o) {
        return dataAscending.compare(this, o);

    }

    public static Comparator<ELE> dataAscending = new Comparator<ELE>() {
        @Override
        public int compare(ELE o1, ELE o2) {
            double diff = o1.data - o2.data;
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    };
    public static Comparator<ELE> dataDescending = new Comparator<ELE>() {
        @Override
        public int compare(ELE o1, ELE o2) {
            double diff = o2.data - o1.data;
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            } else {
                return 0;
            }
        }
    };

}
