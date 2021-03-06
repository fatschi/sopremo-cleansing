/*
 * DuDe - The Duplicate Detection Toolkit
 * 
 * Copyright (C) 2010  Hasso-Plattner-Institut für Softwaresystemtechnik GmbH,
 *                     Potsdam, Germany 
 * 
 * This file is part of DuDe.
 * 
 * DuDe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * DuDe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DuDe.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package eu.stratosphere.sopremo.cleansing.similarity.text;

import java.io.IOException;
import java.io.ObjectInputStream;

import eu.stratosphere.sopremo.cleansing.similarity.SoundEx;
import eu.stratosphere.util.CharSequenceUtil;

/**
 * <code>SoundExSimilarity</code> compares two {@link IJsonNode}s based on a phonetic algorithm for indexing names by
 * sound.
 * 
 * @author Matthias Pohl
 * @author Arvid Heise
 */
public class SoundExSimilarity extends TextSimilarity {
	private transient StringBuilder soundex1 = new StringBuilder(), soundex2 = new StringBuilder();

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.soundex1 = new StringBuilder();
		this.soundex2 = new StringBuilder();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.String,
	 * java.lang.String, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public float getSimilarity(CharSequence text1, CharSequence text2) {
		if (text1.length() == 0 && text2.length() == 0)
			return 1f;
		if (text1.length() == 0 || text2.length() == 0)
			return 0f;

		try {
			SoundEx.generateSoundExInto(text1, this.soundex1);
			SoundEx.generateSoundExInto(text2, this.soundex2);
		} catch (IOException e) {
		}
		if (CharSequenceUtil.equal(this.soundex1, this.soundex2))
			return 1f;

		return 0f;
	}
}
