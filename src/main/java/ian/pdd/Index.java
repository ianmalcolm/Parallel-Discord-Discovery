package ian.pdd;

import java.util.HashMap;

import ian.ISAXIndex.*;

public class Index extends ISAXIndex implements java.io.Serializable {

	private HashMap<Long, Sequence> memo = new HashMap<Long, Sequence>();
	private boolean abandon = false;

	public Index(int maxCardinality, int dimensionality, Distance _df) {
		super(maxCardinality, dimensionality, _df);
		// TODO Auto-generated constructor stub

	}
	
	public Index(Distance _df) {
		super(16, 4, _df);
		// TODO Auto-generated constructor stub
	}

	public void add(double[] seq, long id) {
		super.add(seq, id);
	}

	public void addMemo(long id) {
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
	
	public long cntRefineReq(double _gBSFDist){
		long cnt = 0;
		for (Sequence seq: memo.values()){
			if (seq.reqRefine(_gBSFDist)){
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
}
