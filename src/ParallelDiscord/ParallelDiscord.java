/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ParallelDiscord;

import SAXFactory.DiscordRecords;
import SAXFactory.SAXFactory;
import edu.hawaii.jmotif.sax.datastructures.DiscordRecord;
import edu.hawaii.jmotif.sax.trie.TrieException;
import edu.hawaii.jmotif.timeseries.TSException;
import java.util.ArrayList;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ian
 */
public class ParallelDiscord {

    private static final Logger pdlogger = Logger.getLogger(ParallelDiscord.class.getName());

    static {
        pdlogger.setLevel(Level.OFF);
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.ALL);
        Logger.getLogger("").addHandler(ch);
    }

    public static void setLoggerLevel(Level level) {
        pdlogger.setLevel(level);
    }

    public static DiscordRecords discoverDiscordsInParallel(double[] series, int windowSize, int alphabetSize, int reportNum, int parallelism, boolean PRECISE) throws TSException, TrieException {

        ArrayList<WORKER> workers = new ArrayList();
        DiscordRecords drs = new DiscordRecords(reportNum, windowSize);

        for (int i = 0; i < parallelism; i++) {
            int interval = (series.length - windowSize + 1) / parallelism;
            int start = i * interval;
            int end = start + interval - 1 + windowSize - 1;
            if (i == parallelism - 1) {
                end = series.length;
            }
            double[] subseries = SAXFactory.getSubSeries(series, start, end);
            workers.add(new WORKER(subseries, windowSize, alphabetSize, start, PRECISE));
        }

        int round = 0;
        int discordsNumInCurRound = 0;

        do {
            pdlogger.fine("Round " + round);
            discordsNumInCurRound = 0;
            // find discord
            for (WORKER w : workers) {
                if (w.isAbandoned()) {
                    continue;
                }
                DiscordRecord curDiscord = w.findNextDiscord();
                if (curDiscord != null) {
                    discordsNumInCurRound++;
                    int curDiscordPosition = curDiscord.getPosition() + w.getOffset();
                    if (drs.getKthDistance() > curDiscord.getDistance()) {
                        w.setAbandon();
                    } else {
                        // refine discord
                        double minDist = Double.MAX_VALUE;
                        double[] discordSeries = SAXFactory.getSubSeries(series, curDiscordPosition, curDiscordPosition + windowSize);
                        for (WORKER wo : workers) {
                            if (wo.includes(curDiscordPosition)) {
                                continue;
                            }
                            double kthMinDist = wo.findNNDist(discordSeries);
                            if (minDist > kthMinDist) {
                                minDist = kthMinDist;
                            }
                        }
                        w.refineDiscord(curDiscordPosition, minDist);
                        // aggregate the discord
                        drs.add(new DiscordRecord(curDiscordPosition, minDist));
                    }
                }
            }

            pdlogger.info("Workers found " + discordsNumInCurRound + " more discords in round " + round);
            round++;
        } while (discordsNumInCurRound > 0);

        return drs;
    }
}
