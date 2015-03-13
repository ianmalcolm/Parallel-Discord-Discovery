/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package Test;

import Distance.ED;
import ParallelDiscord.ParallelDiscord;
import static ParallelDiscord.ParallelDiscord.discoverDiscordsInParallel;
import ParallelDiscord.WORKER;
import PreciseDiscord.PreciseDiscord;
import SAXFactory.DiscordRecords;
import SAXFactory.SAXFactory;
import java.util.Date;
import java.util.logging.Level;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import weka.core.Attribute;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;

/**
 *
 * @author ian
 */
public class Test {
        /**
     * The discords experiment code. see the example on the web and more
     * detailed description in the Keogh's article.
     *
     * @author Pavel Senin.
     *
     */
    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here

        Date totalstart = new Date();

        int SLIDING_WINDOW_SIZE = 360;
        int ALPHABET_SIZE = 5;
        String DATA_VALUE_ATTRIBUTE = "value0";
        String FILE = "..\\ecg\\ecg102.arff";
        int PARALLELISM = 1;
        int LENGTH = 64000;
        int REPORT_NUM = 5;
        boolean PRECISE = false;
        Level level = Level.FINE;

        if (args.length > 0) {
            Options options = new Options();
            options.addOption("pre", true, "Discover precise discord");
            options.addOption("len", true, "Set the length of dataset");
            options.addOption("rep", true, "The number of reported discords");
            options.addOption("par", true, "The number of nodes");
            options.addOption("fil", true, "The file name of the dataset");
            options.addOption("att", true, "The abbribute of instances, see the introduction of arff in WEKA for details");
            options.addOption("alp", true, "The size of alphabets");
            options.addOption("win", true, "The size of sliding window");
            options.addOption("log", true, "The size of sliding window");
            options.addOption("h", false, "Print help message");

            CommandLineParser parser = new BasicParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("prd", options);
                return;
            }
            if (cmd.hasOption("alp")) {
                ALPHABET_SIZE = Integer.parseInt(cmd.getOptionValue("alp"));
            }
            if (cmd.hasOption("win")) {
                SLIDING_WINDOW_SIZE = Integer.parseInt(cmd.getOptionValue("win"));
            }
            if (cmd.hasOption("len")) {
                LENGTH = Integer.parseInt(cmd.getOptionValue("len"));
            }
            if (cmd.hasOption("rep")) {
                REPORT_NUM = Integer.parseInt(cmd.getOptionValue("rep"));
            }
            if (cmd.hasOption("par")) {
                PARALLELISM = Integer.parseInt(cmd.getOptionValue("par"));
            }
            if (cmd.hasOption("fil")) {
                FILE = cmd.getOptionValue("fil");
            }
            if (cmd.hasOption("att")) {
                DATA_VALUE_ATTRIBUTE = cmd.getOptionValue("att");
            }
            if (cmd.hasOption("pre")) {
                if (cmd.getOptionValue("pre").equalsIgnoreCase("true")) {
                    PRECISE = true;
                } else {
                    PRECISE = false;
                }
            }
            if (cmd.hasOption("log")) {
                if (cmd.getOptionValue("log").equalsIgnoreCase("OFF")) {
                    level = Level.OFF;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("SEVERE")) {
                    level = Level.SEVERE;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("WARNING")) {
                    level = Level.WARNING;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("INFO")) {
                    level = Level.INFO;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("CONFIG")) {
                    level = Level.CONFIG;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("FINE")) {
                    level = Level.FINE;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("FINER")) {
                    level = Level.FINER;
                } else if (cmd.getOptionValue("log").equalsIgnoreCase("FINEST")) {
                    level = Level.FINEST;
                }
            }
        }

        System.out.println("-fil " + FILE + " -att " + DATA_VALUE_ATTRIBUTE + " -win " + SLIDING_WINDOW_SIZE + " -len " + LENGTH + " -par " + PARALLELISM + " -rep " + REPORT_NUM + " -pre " + PRECISE + " -alp " + ALPHABET_SIZE);

        ParallelDiscord.setLoggerLevel(level);
        PreciseDiscord.setLoggerLevel(level);
        WORKER.setLoggerLevel(level);
        SAXFactory.setLoggerLevel(level);

        DiscordRecords discords;

        // get the data first
        Instances tsData = ConverterUtils.DataSource.read(FILE);
        Attribute dataAttribute = tsData.attribute(DATA_VALUE_ATTRIBUTE);
        double[] timeseries = SAXFactory.toRealSeries(tsData, dataAttribute);

        if (LENGTH
                > 0) {
            timeseries = SAXFactory.getSubSeries(timeseries, 0, LENGTH);
        }

        // find discords
        if (PARALLELISM
                > 1) {
            discords = discoverDiscordsInParallel(timeseries, SLIDING_WINDOW_SIZE, ALPHABET_SIZE, REPORT_NUM, PARALLELISM, PRECISE);
        } else {
            if (PRECISE) {
//                discords = SAXFactory.getBruteForceDiscords(timeseries, SLIDING_WINDOW_SIZE, REPORT_NUM);
                discords = PreciseDiscord.findDiscords(timeseries, SLIDING_WINDOW_SIZE, ALPHABET_SIZE, REPORT_NUM);
            } else {
                discords = SAXFactory.instances2Discords(timeseries, SLIDING_WINDOW_SIZE, ALPHABET_SIZE, REPORT_NUM);
            }
        }

        for (int i = 0; i < discords.getSize(); i++) {
            int start = discords.getKthPosition(i);
            int end = start + SLIDING_WINDOW_SIZE;
            double dist = discords.getKthDistance(i);
            System.out.print(start + "\t" + end + "\t" + dist + "\n");
        }

        Date totalend = new Date();
        System.out.println((totalend.getTime() - totalstart.getTime()) / 1000);
        System.out.println(ED.getCounter());

    }
}
