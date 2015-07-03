package ian.pdd;

import java.util.ArrayList;

public class Filtering implements java.io.Serializable {

	private final DataOfBroadcast dh;
	private final double globDiscord;

	public Filtering(DataOfBroadcast _dh, double _globDiscord) {
		dh = _dh;
		globDiscord = _globDiscord;
	}

	public Long[] filter(Index index) {
		ArrayList<Long> result = new ArrayList<Long>();
		for (Sequence seq : index.memoValues()) {

			if (seq.dist == Double.POSITIVE_INFINITY) {
				// search local nearest neighbor
				ArrayList<Long> selfExcept = new ArrayList<Long>();
				for (long overlap = seq.id - dh.windowSize() + 1; overlap < seq.id
						+ dh.windowSize(); overlap++) {
					selfExcept.add(overlap);
				}
				ArrayList<Long> knn = index.knn(dh.get(seq.id), 1, selfExcept);
				double dist = index.df.distance(dh.get(seq.id),
						dh.get(knn.get(0)));
				index.updateMemo(seq.id, dist, false);
			}

			if (seq.dist <= globDiscord) {
				result.add(seq.id);
			}
		}

		for (Long id : result) {
			index.removeMemo(id);
		}
		return result.toArray(new Long[result.size()]);
	}

}
