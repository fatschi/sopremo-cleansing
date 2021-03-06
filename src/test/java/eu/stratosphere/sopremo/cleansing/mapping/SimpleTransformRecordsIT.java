/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class SimpleTransformRecordsIT extends MeteorIT {
	private File usCongressMembers, usCongressBiographies, person, legalEntity;

	@Before
	public void createFiles() throws IOException {
		this.usCongressMembers = this.testServer.createFile("usCongressMembers.json", 
				createObjectNode("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029"),
				createObjectNode("id", "usCongress2", "name", "John Adams", "biography", "A000039"), 
				createObjectNode("id", "usCongress3", "name", "John Doe", "biography", "A000059")
				);
		this.usCongressBiographies = this.testServer.createFile("usCongressBiographies.json", 
				createObjectNode("biographyId", "A000029", "worksFor", "CompanyXYZ"),
				createObjectNode("biographyId", "A000059", "worksFor", "CompanyUVW"),
				createObjectNode("biographyId", "A000049", "worksFor", "CompanyABC")
				);
		this.person = this.testServer.getOutputFile("person.json");
		this.legalEntity = this.testServer.getOutputFile("legalEntity.json");
	}
	
	@Test
	public void testSimpleMapping() throws IOException {

		String query = "using cleansing;"+
				"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
				"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
				"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
				"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" + 
				"into [\n" +  
				"  entity $person with {" + // identified by $person.pname
				"    pname: $usCongressMembers.name,\n" +
				"    pworksFor: $legalEntity.id" + 
				"  }," + 
				"  entity $legalEntity identified by $legalEntity.lname with {" + 
				"    lname: $usCongressBiographies.worksFor" + 
				"  }" + 
				"];\n" + 
				"write $person to '" + this.person.toURI() + "';\n" +
				"write $legalEntity to '" + this.legalEntity.toURI() + "';";
		
		final SopremoPlan plan = parseScript(query);
		
		SopremoUtil.trace();
		Assert.assertNotNull(this.client.submit(plan, null, true));
		
		this.testServer.checkContentsOf("person.json",
				createObjectNode("id", "Andrew Adams", "pname", "Andrew Adams", "pworksFor", "CompanyXYZ"),
				createObjectNode("id", "John Adams", "pname", "John Adams", "pworksFor", null),
				createObjectNode("id", "John Doe", "pname", "John Doe", "pworksFor", "CompanyUVW"));
		
		this.testServer.checkContentsOf("legalEntity.json",
				createObjectNode("id", "CompanyXYZ", "lname", "CompanyXYZ"),
				createObjectNode("id", "CompanyUVW", "lname", "CompanyUVW"),
				createObjectNode("id", "CompanyABC", "lname", "CompanyABC"));
	}
}