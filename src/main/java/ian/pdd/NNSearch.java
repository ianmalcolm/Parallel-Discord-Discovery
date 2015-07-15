package ian.pdd;

import java.util.ArrayList;

public class NNSearch implements java.io.Serializable {

	private final DataOfBroadcast dh;
	private ArrayList<Sequence> seqs = new ArrayList<Sequence>();
	private double range = Double.NEGATIVE_INFINITY;
	private final int partitionSize;
	public long dfCnt = 0;

	public NNSearch(DataOfBroadcast _dh, Long[] _ld, double _range, int _ps) {
		// TODO Auto-generated constructor stub

		dh = _dh;

		for (long id : _ld) {
			seqs.add(new Sequence(id));
		}

		range = _range;
		partitionSize = _ps;
	}

	public NNSearch(DataOfBroadcast _dh, Long[] _ld) {
		// TODO Auto-generated constructor stub
		dh = _dh;
		for (long id : _ld) {
			seqs.add(new Sequence(id));
		}
		partitionSize = 0;
	}

	public Sequence[] localSearch(int partId, Index index) {

		ArrayList<Sequence> discard = new ArrayList<Sequence>();
		ArrayList<Sequence> result = new ArrayList<Sequence>();

		for (Sequence seq : seqs) {

			if (seq.dist < range) {
				discard.add(seq);
				continue;
			}

			// search local nearest neighbor
			ArrayList<Long> selfExcept = new ArrayList<Long>();
			for (long overlap = seq.id - dh.windowSize() + 1; overlap < seq.id
					+ dh.windowSize(); overlap++) {
				selfExcept.add(overlap);
			}
			ArrayList<Sequence> knn = index.knn(dh.get(seq.id), 1, dh,
					selfExcept, range);

			if (knn == null) {
				discard.add(seq);
				continue;
			}
			if (seq.dist > knn.get(0).dist) {
				seq.set(knn.get(0).neighbor, knn.get(0).dist);
			}
			if (seq.dist < range) {
				discard.add(seq);
				continue;
			}
			seq.partCnt.add(partId);
			if (seq.partCnt.size() == partitionSize) {
				result.add(seq);
				discard.add(seq);
			}
		}

		// System.out.println(localDiscord.size() + "\t" + discard.size());
		seqs.removeAll(discard);

		dfCnt += index.df.getCount();
		return result.toArray(new Sequence[result.size()]);
	}

	public Sequence[] globSearch(Index index) {

		int num = 0;
		for (Sequence seq : seqs) {

			// search local nearest neighbor
			ArrayList<Long> selfExcept = new ArrayList<Long>();
			for (long overlap = seq.id - dh.windowSize() + 1; overlap < seq.id
					+ dh.windowSize(); overlap++) {
				selfExcept.add(overlap);
			}

			ArrayList<Sequence> knn = index.knn(dh.get(seq.id), 1, dh,
					selfExcept, Double.NEGATIVE_INFINITY);

			assert knn != null;
			seq.set(knn.get(0).neighbor,
					index.df.distance(dh.get(seq.id),
							dh.get(knn.get(0).neighbor)));

		}

		dfCnt += index.df.getCount();
		return seqs.toArray(new Sequence[seqs.size()]);
	}

	public int numSeqs() {
		return seqs.size();
	}

	public void setRange(double _range) {
		range = _range;
	}

	public void reload(Long[] _in) {
		for (long id : _in) {
			seqs.add(new Sequence(id));
		}
	}

}
