package ian.pdd;

import java.util.HashSet;

class Sequence implements java.io.Serializable {
	/**
	 * 
	 */
	final long id;
	double dist = Double.POSITIVE_INFINITY;
	long neighbor = Long.MIN_VALUE;
	HashSet<Integer> partCnt = new HashSet<Integer>();

	//
	// public Sequence(){
	//
	// }
	public Sequence(long _id) {
		id = _id;
	}

	public Sequence(long _id, long _nb, double _dist) {
		dist = _dist;
		id = _id;
		neighbor = _nb;
	}

	public void set(long _nb, double _dist) {
		neighbor = _nb;
		dist = _dist;
	}

	public String toString() {
		return id + "\t" + neighbor + "\t" + dist;
	}

}
