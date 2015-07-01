package ian.pdd;

import java.util.ArrayList;
import org.junit.Assert;

public class Detection implements java.io.Serializable {

	private final double gBSFDist;
	private final DataOfBroadcast dh;
	private final ArrayList<Long> exception;
	final static long ABANDON = Long.MIN_VALUE;

	public Detection(DataOfBroadcast _dh, double _gBSFDist) {
		dh = _dh;
		gBSFDist = _gBSFDist;
		exception = new ArrayList<Long>();
	}

	public Detection(DataOfBroadcast _dh, double _gBSFDist, Long[] _exception) {
		dh = _dh;
		gBSFDist = _gBSFDist;
		exception = new ArrayList<Long>();
		for (Long inst : _exception) {
			for (long i = inst - dh.windowSize() + 1; i < inst
					+ dh.windowSize(); i++)
				exception.add(i);
		}
	}

	public long detect(Index index) {

		long lBSFPos = ABANDON;
		double lBSFDist = gBSFDist;

		for (long i : index) {

			// exclude the sequence that has been identified as discord
			if (exception.contains(i)) {
				assert (false);
				continue;
			}

			if (!index.containsMemo(i)) {
				// skip the sequence if it is originally not in this partition
				assert (false);
				continue;
			}

			if (index.getMemoDist(i) < lBSFDist) {
				continue;
			}

			if (!index.isMemoGlob(i)) {

				// search local nearest neighbor
				ArrayList<Long> selfExcept = new ArrayList<Long>();
				for (long overlap = i - dh.windowSize() + 1; overlap < i
						+ dh.windowSize(); overlap++) {
					selfExcept.add(overlap);
				}
				ArrayList<Long> knn = index.knn(dh.get(i), 1, selfExcept);

				for (Long j : knn) {

					Assert.assertTrue(
							"Engaging pair-comparison of overlapped sequence ("
									+ i + "," + j + ")",
							Math.abs(i - j) >= dh.windowSize());

					// Compute the Euclidean Distance
					double curDist = index.df.distance(dh.get(i), dh.get(j));

					index.updateMemo(i, curDist, false);
					// Suppose we use symmetric distance function
					index.updateMemo(j, curDist, false);

					if (index.getMemoDist(i) < lBSFDist) {
						break;
					}
				}
			}

			if (lBSFDist <= index.getMemoDist(i)) {
				lBSFDist = index.getMemoDist(i);
				lBSFPos = i;
			}
		}

		if (index.isMemoGlob(lBSFPos) || lBSFPos == ABANDON) {
			lBSFPos = ABANDON;
			index.setAbandon();
		}

		return lBSFPos;

	}

}
