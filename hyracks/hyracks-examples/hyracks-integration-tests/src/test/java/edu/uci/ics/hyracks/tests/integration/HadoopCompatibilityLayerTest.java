/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;
import edu.uci.ics.hyracks.hadoop.compat.driver.NewCompatibilityLayer;
import edu.uci.ics.hyracks.hadoop.compat.driver.CompatibilityLayer;

public class HadoopCompatibilityLayerTest extends AbstractIntegrationTest {
    @Test
    public void runCompatibilityTest() throws Exception {
        NewCompatibilityLayer.main(new String[] { "-cluster", getClusterConf(), "-jobFiles", getWordCountJobFile(),
                "-applicationName", "compat" });
//        CompatibilityLayer.main(new String[] { "-cluster", getClusterConf(), "-jobFiles", getWordCountJobFile(),
//                "-applicationName", "compat" });
    }

    private String getClusterConf() {
        return new File("conf/local_cluster.conf").getAbsolutePath();
    }

    private String getWordCountJobFile() {
//      return new File("job/wordcountOldAPI.job").getAbsolutePath();
      return new File("job/wordcountNewAPI.job").getAbsolutePath();
    }
}