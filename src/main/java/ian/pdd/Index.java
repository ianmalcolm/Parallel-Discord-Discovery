package ian.pdd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import ian.ISAXIndex.*;

public class Index extends ISAXIndex implements java.io.Serializable {

	private HashMap<Long, Sequence> memo = new HashMap<Long, Sequence>();
	private boolean abandon = false;

	public Index(int maxCardinality, int dimensionality, Distance _df) {
		super(maxCardinality, dimensionality, _df);
		// TODO Auto-generated constructor stub

	}

	public Index(Distance _df) {
		super(8, 4, _df);
		// TODO Auto-generated constructor stub
	}

	public void add(double[] seq, long id) {
		super.add(seq, id);
		memo.put(id, new Sequence(id));

	}

	public void updateMemo(Long id, double dist, boolean glob) {
		if (memo.containsKey(id)) {
			if (memo.get(id).glob == false) {
				if (glob == true) {
					memo.get(id).dist = dist;
					memo.get(id).glob = glob;
				} else {
					if (memo.get(id).dist > dist) {
						memo.get(id).dist = dist;
					}
				}
			}
		}
		// else {
		// memo.put(id, new Sequence(dist, glob));
		// }
	}

	public double getMemoDist(Long id) {
		if (memo.containsKey(id)) {
			return memo.get(id).dist;
		} else {
			return Double.POSITIVE_INFINITY;
		}
	}

	public boolean containsMemo(Long id) {
		return memo.containsKey(id);
	}

	public long getMemoNeighbor(Long id) {
		if (memo.containsKey(id)) {
			return memo.get(id).neighbor;
		} else {
			return Detection.ABANDON;
		}
	}

	public boolean isMemoGlob(Long id) {
		if (memo.containsKey(id)) {
			return memo.get(id).glob;
		} else {
			return false;
		}
	}

	public Collection<Sequence> memoValues() {
		return memo.values();
	}

	public void removeMemo(Long id) {
		memo.remove(id);
	}

	public long cntRefineReq(double _gBSFDist) {
		long cnt = 0;
		for (Sequence seq : memo.values()) {
			if (seq.reqRefine(_gBSFDist)) {
				cnt++;
			}
		}
		return cnt;
	}

	public boolean isAbandon() {
		return abandon;
	}

	public void setAbandon() {
		abandon = true;
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
			for (Long id : occu){
				occus.add(id);
			}
		}
		return occus.toArray(new Long[occus.size()]);
	}
}
