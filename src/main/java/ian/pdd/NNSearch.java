package ian.pdd;

import java.util.ArrayList;

import org.apache.spark.broadcast.Broadcast;

public class NNSearch implements java.io.Serializable {

	private final DataOfBroadcast dh;
	private ArrayList<Sequence> seqs = new ArrayList<Sequence>();
	private double range = Double.NEGATIVE_INFINITY;
	public long dfCnt = 0;
	private Broadcast<Index> bcIndex;

	public NNSearch(DataOfBroadcast _dh, Broadcast<Index> bc, Long[] _ld,
			double _range) {
		// TODO Auto-generated constructor stub

		dh = _dh;

		for (long id : _ld) {
			seqs.add(new Sequence(id));
		}

		range = _range;
		bcIndex = bc;
	}

	public NNSearch(DataOfBroadcast _dh, Long[] _ld, Broadcast<Index> bc) {
		// TODO Auto-generated constructor stub
		dh = _dh;
		for (long id : _ld) {
			seqs.add(new Sequence(id));
		}
		bcIndex = bc;
	}

	public Sequence localSearch() {
		Sequence result = seqs.get(0);

		for (Sequence seq : seqs) {

			// search local nearest neighbor
			ArrayList<Long> selfExcept = new ArrayList<Long>();
			for (long overlap = seq.id - dh.windowSize() + 1; overlap < seq.id
					+ dh.windowSize(); overlap++) {
				selfExcept.add(overlap);
			}

			ArrayList<Long> knn = bcIndex.value().knn(dh.get(seq.id), 1, dh,
					selfExcept, range);
			double dist = bcIndex.value().df.distance(dh.get(seq.id),
					dh.get(knn.get(0)));

			seq.set(knn.get(0), dist);
			if (result.dist < seq.dist) {
				result = seq;
			}
		}

		seqs.clear();
		dfCnt += bcIndex.value().df.getCount();
		return result;
	}

	public Sequence globSearch() {

		Sequence result = seqs.get(0);
		for (Sequence seq : seqs) {

			// search local nearest neighbor
			ArrayList<Long> selfExcept = new ArrayList<Long>();
			for (long overlap = seq.id - dh.windowSize() + 1; overlap < seq.id
					+ dh.windowSize(); overlap++) {
				selfExcept.add(overlap);
			}

			ArrayList<Long> knn = bcIndex.value().knn(dh.get(seq.id), 1, dh,
					selfExcept);
			double dist = bcIndex.value().df.distance(dh.get(seq.id),
					dh.get(knn.get(0)));
			seq.set(knn.get(0), dist);
			if (result.dist < seq.dist) {
				result = seq;
			}
		}

		return result;
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
