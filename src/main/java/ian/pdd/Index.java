package ian.pdd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import ian.ISAXIndex.*;

public class Index extends ISAXIndex implements java.io.Serializable {

	public Index(int maxCardinality, int dimensionality) {
		super(maxCardinality, dimensionality, new ED());
		// TODO Auto-generated constructor stub

	}

	public void add(double[] seq, long id) {
		super.add(seq, id);

	}

	// exception aware approximated nearest neighbor
	public ArrayList<Sequence> knn(double[] vals, int k, DataHandler dh,
			ArrayList<Long> exception, double range) {

		KNNSequence knn = new KNNSequence(k, exception);

		ISAX q = new ISAX(vals, dimension, 1 << maxWidth);
		ArrayList<Node> candidates = new ArrayList<Node>();
		candidates.add(root);
		while (!candidates.isEmpty()) {
			Node n = candidates.get(0);
			candidates.remove(0);
			if (n.isLeaf()) {
				Iterator<Long> iter = ((Leaf) n).iterator();
				while (iter.hasNext()) {
					long id = iter.next();
					if (knn.contains(id) || exception.contains(id)) {
						continue;
					}
					double dist = df.distance(vals, dh.get(id));
					if (dist <= knn.kDist()) {
						knn.add(id, dist);
					}
					if (knn.kDist() <= range) {
						return null;
					}
				}
			} else {
				double dist = n.minDist(q);
				if (dist <= knn.kDist()) {
					Iterator<Node> iter = n.iterator();
					while (iter.hasNext()) {
						candidates.add(iter.next());
					}
				}
			}
		}
		return knn.toArrayList();
	}

	public Long[] getOccurences(String[] words) {
		ArrayList<Long> occus = new ArrayList<Long>();
		for (String word : words) {
			Long[] occu = getOccurence(word);
			for (Long id : occu) {
				occus.add(id);
			}
		}
		return occus.toArray(new Long[occus.size()]);
	}

	protected class KNNSequence {

		private ArrayList<Sequence> list = new ArrayList<Sequence>();
		private int k = 0;
		private ArrayList<Long> exception = null;

		public KNNSequence(int _k, ArrayList<Long> _exception) {
			k = _k;
			exception = _exception;
		}

		public boolean add(Long id, double dist) {

			if (contains(id)) {
				return false;
			}
			if (exception != null) {
				if (exception.contains(id)) {
					return false;
				}
			}
			Sequence o = new Sequence(Long.MIN_VALUE, id, dist);
			int position = Collections.binarySearch(list, o);
			if (position < 0) {
				list.add(-1 * position - 1, o);
			} else {
				list.add(position, o);
			}
			return true;
		}

		public boolean contains(Long id) {
			for (Sequence seq : list) {
				if (seq.neighbor == id) {
					return true;
				}
			}
			return false;
		}

		public double kDist() {
			if (numID() < k) {
				return Double.MAX_VALUE;
			} else {
				return list.get(k - 1).dist;
			}
		}

		public int numID() {
			return list.size();
		}

		public ArrayList<Sequence> toArrayList() {
			return list;
		}
	}
}
