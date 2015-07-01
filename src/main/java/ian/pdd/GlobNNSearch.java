package ian.pdd;

import java.util.ArrayList;

import org.junit.Assert;

public class GlobNNSearch implements java.io.Serializable {

	private final DataOfBroadcast dh;
	private final Long[] localDiscord;

	public GlobNNSearch(DataOfBroadcast _dh, Long[] _ld) {
		// TODO Auto-generated constructor stub
		dh = _dh;
		localDiscord = _ld;
		for (long ld : localDiscord) {
			Assert.assertTrue(
					"Engaging invalid sequence in GlobNNSearch phase!", ld >= 0
							&& ld < dh.size());
		}
	}

	public Sequence[] search(Index index) {
		Sequence[] result = new Sequence[localDiscord.length];

		// for (int h = 0; h < localDiscord.length; h++) {
		int num = 0;
		for (long i : localDiscord) {

			Assert.assertTrue("Search NN distance of the global sequence " + i,
					!index.isMemoGlob(i));
			Assert.assertTrue(
					"Engaging an invalid sequence in GLobNNSearch phase!",
					i != Detection.ABANDON);

			// search local nearest neighbor
			ArrayList<Long> selfExcept = new ArrayList<Long>();
			for (long overlap = i - dh.windowSize() + 1; overlap < i
					+ dh.windowSize(); overlap++) {
				selfExcept.add(overlap);
			}

			ArrayList<Long> knn = index.knn(dh.get(i), 1, dh, selfExcept);
			result[num++] = new Sequence(i, knn.get(0), index.df.distance(
					dh.get(i), dh.get(knn.get(0))), false);
//			System.out.println(result[num - 1].toString());

			// ArrayList<Long> knn = index.knn(dh.get(i), 1, selfExcept);
			// double nnDist = Double.POSITIVE_INFINITY;
			// long nnNb = Detection.ABANDON;
			//
			// for (long j : knn) {
			// Assert.assertTrue(
			// "Engaging pair-comparison of overlapped sequence (" + i
			// + "," + j + ")",
			// Math.abs(i - j) >= dh.windowSize());
			//
			// // Compute the Euclidean Distance
			// double curDist = index.df.distance(dh.get(i), dh.get(j));
			//
			// if (nnDist > curDist) {
			// nnDist = curDist;
			// nnNb = j;
			// }
			//
			// }
			//
			// Assert.assertTrue("Cannot find any neighbor of " + i + ".",
			// nnNb != Detection.ABANDON);
			// Assert.assertTrue("Strange local distance " + nnDist
			// + " of the sequence " + i, nnDist > 0
			// && nnDist < Double.MAX_VALUE);
			//
			// result[num++] = new Sequence(i, nnNb, nnDist, false);

		}

		return result;
	}
}
