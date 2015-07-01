package ian.pdd;

class Sequence implements java.io.Serializable {
	long id = Detection.ABANDON;
	double dist = Double.POSITIVE_INFINITY;
	boolean glob = false;
	long neighbor = Detection.ABANDON;

	//
	// public Sequence(){
	//
	// }
	public Sequence(long _id) {
		id = _id;
	}

	public Sequence(double _dist, boolean _glob) {
		dist = _dist;
		glob = _glob;
	}

	public Sequence(long _id, long _nb, double _dist, boolean _glob) {
		dist = _dist;
		glob = _glob;
		id = _id;
		neighbor = _nb;
	}

	public boolean reqRefine(double _gBSFDist) {
		if (dist > _gBSFDist) {
			return true;
		} else {
			return false;
		}
	}

	public String toString() {
		return id + "\t" + neighbor + "\t" + dist + "\t" + glob;
	}

	// public void set(long _id, long _nb, double _dist, boolean _glob) {
	// dist = _dist;
	// glob = _glob;
	// id = _id;
	// neighbor = _nb;
	// }
}
