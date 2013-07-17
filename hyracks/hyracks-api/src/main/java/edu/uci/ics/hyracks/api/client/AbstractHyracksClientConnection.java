package edu.uci.ics.hyracks.api.client;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.hyracks.api.client.impl.JobSpecificationActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public abstract class AbstractHyracksClientConnection implements IHyracksClientConnection {
    protected String ccHost;

    protected ClusterControllerInfo ccInfo;

    @Override
    public final JobId startJob(JobSpecification jobSpec) throws Exception {
        return startJob(jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public final JobId startJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        return startJob(null, jobSpec, jobFlags);
    }

    public final JobId startJob(IActivityClusterGraphGeneratorFactory acggf, EnumSet<JobFlag> jobFlags)
            throws Exception {
        return startJob(null, acggf, jobFlags);
    }

    @Override
    public final DeploymentId deployBinary(List<String> jars) throws Exception {
        /** generate a deployment id */
        DeploymentId deploymentId = new DeploymentId(UUID.randomUUID().toString());
        List<URL> binaryURLs = new ArrayList<URL>();
        if (jars != null && jars.size() > 0) {
            HttpClient hc = new DefaultHttpClient();
            /** upload jars through a http client one-by-one to the CC server */
            for (String jar : jars) {
                int slashIndex = jar.lastIndexOf('/');
                String fileName = jar.substring(slashIndex + 1);
                String url = "http://" + ccHost + ":" + ccInfo.getWebPort() + "/applications/"
                        + deploymentId.toString() + "&" + fileName;
                HttpPut put = new HttpPut(url);
                put.setEntity(new FileEntity(new File(jar), "application/octet-stream"));
                HttpResponse response = hc.execute(put);
                if (response != null) {
                    response.getEntity().consumeContent();
                }
                if (response.getStatusLine().getStatusCode() != 200) {
                    unDeployBinary(deploymentId);
                    throw new HyracksException(response.getStatusLine().toString());
                }
                /** add the uploaded URL address into the URLs of jars to be deployed at NCs */
                binaryURLs.add(new URL(url));
            }
        }
        /** deploy the URLs to the CC and NCs */
        performDeployment(binaryURLs, deploymentId);
        return deploymentId;
    }

    protected abstract void performDeployment(List<URL> binaryURLs, DeploymentId deploymentId) throws Exception;

    @Override
    public final JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec) throws Exception {
        return startJob(deploymentId, jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public final JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags)
            throws Exception {
        JobSpecificationActivityClusterGraphGeneratorFactory jsacggf = new JobSpecificationActivityClusterGraphGeneratorFactory(
                jobSpec);
        return startJob(deploymentId, jsacggf, jobFlags);
    }
}