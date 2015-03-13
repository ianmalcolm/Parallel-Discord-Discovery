/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package PreciseDiscord;

import SAXFactory.DiscordRecords;
import SAXFactory.SAXFactory;
import Distance.ED;
import edu.hawaii.jmotif.sax.LargeWindowAlgorithm;
import edu.hawaii.jmotif.sax.SlidingWindowMarkerAlgorithm;
import edu.hawaii.jmotif.sax.alphabet.NormalAlphabet;
import edu.hawaii.jmotif.sax.datastructures.DiscordRecord;
import edu.hawaii.jmotif.sax.trie.SAXTrie;
import edu.hawaii.jmotif.sax.trie.SAXTrieHitEntry;
import edu.hawaii.jmotif.sax.trie.TrieException;
import edu.hawaii.jmotif.sax.trie.VisitRegistry;
import edu.hawaii.jmotif.timeseries.TSException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PreciseDiscord {

//    private final TreeMap<String, DistanceEntry> knownWordsAndTheirCurrentDistances = new TreeMap<>();
//    private final TreeSet<String> completeWords = new TreeSet<>();
//    private final ArrayList<String> wordList;
    private static final Logger pdlogger = Logger.getLogger(PreciseDiscord.class.getName());

    static {
        pdlogger.setLevel(Level.OFF);
    }

    private ArrayList<String> getAllWords(SAXTrie _trie) {
        ArrayList<SAXTrieHitEntry> freq = _trie.getFrequencies();
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

    public static void setLoggerLevel(Level level) {
        pdlogger.setLevel(level);
    }

    // return all non-repeated words in the order the of input arraylist
    private static ArrayList<String> getAllWords(ArrayList<SAXTrieHitEntry> _freq) {
        ArrayList<String> words = new ArrayList();
        for (int i = 0; i < _freq.size(); i++) {
            String word = new String(_freq.get(i).getStr());
            if (words.contains(word)) {
                continue;
            }
            words.add(word);
        }
        return words;
    }

    public static DiscordRecords findDiscords(double[] series, int windowSize, int alphabetSize, int reportNum) throws TSException, TrieException {

        SlidingWindowMarkerAlgorithm marker = new LargeWindowAlgorithm();
        VisitRegistry discordsVisitRegistry = new VisitRegistry(series.length - windowSize);
        NormalAlphabet normalA = new NormalAlphabet();
        SAXTrie trie = new SAXTrie(series.length - windowSize + 1, alphabetSize);
        double[] NNDists = new double[series.length - windowSize + 1];
        for (int i = 0;
                i < NNDists.length;
                i++) {
            NNDists[i] = Double.MAX_VALUE;
        }

        for (int i = 0; i < series.length - windowSize + 1; i++) {
            double[] subSeries = SAXFactory.getSubSeries(series, i, i + windowSize);
            char[] saxVals = SAXFactory.getSaxVals(subSeries, windowSize, normalA.getCuts(alphabetSize));
            trie.put(String.valueOf(saxVals), i);
        }

        pdlogger.fine("starting discords finding routines");
        int ttDistFuncCnt = 0;
        DiscordRecords drs = new DiscordRecords(reportNum);

        while (drs.getSize()
                < reportNum) {
            pdlogger.finer("Currently known discords: " + drs.getSize() + " out of " + reportNum);

            int[] distFuncCnt = new int[]{0};
            Date start = new Date();
            DiscordRecord bestDiscord = findNextDiscord(series, windowSize, trie, discordsVisitRegistry, NNDists, distFuncCnt);
            Date end = new Date();

            // if the discord is null we getting out of the search            
            if (bestDiscord.getDistance() == 0.0D || bestDiscord.getPosition() == -1) {
                pdlogger.fine("breaking the outer search loop, discords found: " + drs.getSize()
                        + " last seen discord: " + bestDiscord.toString());
                return drs;
            }

            // collect the result
            //
            drs.add(bestDiscord);

            pdlogger.fine("Find #" + drs.getSize() + " discord: " + bestDiscord.getPayload() + " at "
                    + bestDiscord.getPosition() + ", distance " + bestDiscord.getDistance()
                    + ", elapsed time: " + SAXFactory.timeToString(start.getTime(), end.getTime()) + ", #distfunc: " + distFuncCnt[0]);

            // and maintain data structures
            //
            marker.markVisited(discordsVisitRegistry, bestDiscord.getPosition(), windowSize);
//            completeWords.add(String.valueOf(bestDiscord.getPayload()));

            ttDistFuncCnt = ttDistFuncCnt + distFuncCnt[0];
        }
        return drs;
    }

    public static DiscordRecord findNextDiscord(double[] series, int windowSize, SAXTrie trie, VisitRegistry discordsVisitRegistry, double[] NNDists, int[] distanceFunctionCounter) throws TSException, TrieException {

        ArrayList<SAXTrieHitEntry> frequencies = trie.getFrequencies();
        Collections.sort(frequencies);
        ArrayList<String> allOuterWords = getAllWords(frequencies);
        ArrayList<String> allInnerWords = getAllWords(frequencies);

        double bestSoFarDistance = 0.0D;
        int bestSoFarPosition = -1;
        String bestSoFarString = "";

        // outer loop
        for (int i = 0; i < allOuterWords.size(); i++) {
            char[] outerWord = allOuterWords.get(i).toCharArray();
            sortWordsBySimilar(outerWord, allInnerWords, trie.getAlphabetSize());
            List<Integer> outerOccurences = trie.getOccurences(outerWord);
            for (int outerOccurence : outerOccurences) {

                // if already visited, then continue
                if (discordsVisitRegistry.isVisited(outerOccurence)) {
                    continue;
                }
                double[] outerSeries = SAXFactory.getSubSeries(series, outerOccurence, outerOccurence + windowSize);
                boolean completeSearch = true;

                // inner loop
                for (int j = 0; j < allInnerWords.size(); j++) {
                    char[] innerWord = allInnerWords.get(j).toCharArray();
                    List<Integer> innerOccurences = trie.getOccurences(innerWord);
                    for (int innerOccurence : innerOccurences) {

                        // if overlapped, then continue
                        if ((Math.abs(innerOccurence - outerOccurence) < windowSize)) {
                            continue;
                        }

                        // compute distance
                        double[] innerSeries = SAXFactory.getSubSeries(series, innerOccurence, innerOccurence + windowSize);
                        double currentDistance = ED.distance(outerSeries, innerSeries);
                        distanceFunctionCounter[0]++;

                        // update the nearest neighbor distance of the current outer occurence
                        if (currentDistance < NNDists[outerOccurence]) {
                            NNDists[outerOccurence] = currentDistance;
                        }

                        // early abandoning of the search, the current ocurrence is not discord, we seen better
                        if (NNDists[outerOccurence] < bestSoFarDistance) {
                            pdlogger.finest(" ** abandoning random visits loop, seen distance " + NNDists[outerOccurence] + " at location " + innerOccurence);
                            completeSearch = false;
                            break;
                        }
                    }
                    if (!completeSearch) {
                        break;
                    }
                } // inner loop end

                if (completeSearch) {    // means we found a true best so far NNDist, so time to update bestSoFar stuff
                    bestSoFarPosition = outerOccurence;
                    bestSoFarDistance = NNDists[outerOccurence];
                    bestSoFarString = new String(outerWord);
                }
            }
        } // outer loop end

        return new DiscordRecord(bestSoFarPosition, bestSoFarDistance, bestSoFarString);
    }

    // The most similar word appears at the beginning of the wordList
    private static ArrayList<String> sortWordsBySimilar(char[] inputword, ArrayList<String> allWords, int alphabetSize) throws TSException {
        NormalAlphabet normalA = new NormalAlphabet();
        double[][] distMatrix = normalA.getDistanceMatrix(alphabetSize);
        ArrayList<Double> distList = new ArrayList();
        for (String word : allWords) {
            distList.add(SAXFactory.saxMinDist(word.toCharArray(), inputword, distMatrix));
        }
        for (int i = 0; i < distList.size() - 1; i++) {
            for (int j = 0; j < distList.size() - i - 1; j++) {
                if (distList.get(j) > distList.get(j + 1)) {
                    String oswap = allWords.get(j);
                    double dswap = distList.get(j);
                    allWords.set(j, allWords.get(j + 1));
                    distList.set(j, distList.get(j + 1));
                    allWords.set(j + 1, oswap);
                    distList.set(j + 1, dswap);
                }
            }
        }
        return allWords;
    }
}
