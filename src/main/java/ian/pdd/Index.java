package ian.pdd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import ian.ISAXIndex.*;

public class Index extends ISAXIndex implements java.io.Serializable {

	public Index(int maxCardinality, int dimensionality, Distance _df) {
		super(maxCardinality, dimensionality, _df);
		// TODO Auto-generated constructor stub

	}

	public void add(double[] seq, long id) {
		super.add(seq, id);

	}

	// exception aware approximated nearest neighbor
	public ArrayList<Long> knn(double[] vals, int k, DataHandler dh,
			ArrayList<Long> exception, double range) {

		ArrayList<Long> results = knn(vals, k, exception);
		KNNID knn = new KNNID(k, exception);
		for (Long id : results) {
			double dist = df.distance(vals, dh.get(id));
			knn.add(id, dist);
		}

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
						return knn.toArrayListLong();
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
		return knn.toArrayListLong();
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
}
