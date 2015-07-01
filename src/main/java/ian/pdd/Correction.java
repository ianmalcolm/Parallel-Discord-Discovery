package ian.pdd;

import org.junit.Assert;

public class Correction implements java.io.Serializable {

	private final DataOfBroadcast dh;
	private final Sequence[] globSeqs;

	public Correction(DataOfBroadcast _dh, Sequence[] _globSeqs) {
		dh = _dh;
		globSeqs = _globSeqs;
	}

	public void correct(Index index) {

		for (Sequence seq : globSeqs) {

			Assert.assertTrue("Error id " + seq.id + " in correction phase",
					seq.id >= 0 && seq.id < dh.size());
			Assert.assertTrue("Error neighbor " + seq.id
					+ " in correction phase", seq.neighbor >= 0
					&& seq.neighbor < dh.size());
			Assert.assertTrue(
					"Error dist " + seq.dist + " in correction phase",
					seq.dist >= 0 && seq.dist < Double.MAX_VALUE);
			// Assert.assertTrue(
			// "Error blob " + seq.glob + " in correction phase",
			// seq.glob == true);

			if (index.containsMemo(seq.id)) {
				index.updateMemo(seq.id, seq.dist, seq.glob);
				index.add(dh.get(seq.neighbor), seq.neighbor);
			}
		}
	}

}
