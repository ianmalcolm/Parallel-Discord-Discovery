package ian.pdd;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;

public class NNSearch implements java.io.Serializable {

	private final DataOfBroadcast dh;
	private ArrayList<Sequence> seqs = new ArrayList<Sequence>();
	private double range = Double.NEGATIVE_INFINITY;
	private final int partitionSize;
	private int busyThreshold;

	public NNSearch(DataOfBroadcast _dh, Long[] _ld, double _range, int _ps,
			int _bt) {
		// TODO Auto-generated constructor stub

		dh = _dh;

		for (long id : _ld) {
			seqs.add(new Sequence(id));
		}

		range = _range;
		partitionSize = _ps;
		busyThreshold = _bt;
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

		// for (int h = 0; h < localDiscord.length; h++) {
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

			ArrayList<Long> knn = index.knn(dh.get(seq.id), 1, dh, selfExcept,
					range);
			double dist = index.df.distance(dh.get(seq.id), dh.get(knn.get(0)));
			seq.partCnt.add(partId);

			if (seq.dist > dist) {
				seq.set(knn.get(0), dist);
			}
			if (seq.dist < range) {
				discard.add(seq);
				continue;
			} else if (seq.partCnt.size() == partitionSize) {
				result.add(seq);
				discard.add(seq);
			}
		}

		// System.out.println(localDiscord.size() + "\t" + discard.size());
		seqs.removeAll(discard);

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

			ArrayList<Long> knn = index.knn(dh.get(seq.id), 1, dh, selfExcept);
			seq.set(knn.get(0),
					index.df.distance(dh.get(seq.id), dh.get(knn.get(0))));
			// System.out.println(result[num - 1].toString());

		}

		return seqs.toArray(new Sequence[seqs.size()]);
	}

	public int numSeqs() {
		return seqs.size();
	}

	public boolean isBusy() {
		return seqs.size() > busyThreshold;
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
