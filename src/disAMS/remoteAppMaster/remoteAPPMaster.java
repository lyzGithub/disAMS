package disAMS.remoteAppMaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
//modify import
import disAMS.AMRMClient.AMRMClient;
import disAMS.AMRMClient.AMRMClient.ContainerRequest;
//import org.apache.hadoop.yarn.client.api.AMRMClient;
//import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public class remoteAPPMaster {

	private Configuration conf;// yarn conf
	private AMRMClient<ContainerRequest> rmClient;// client from am to rm
	private NMClient nmClient;// client from am o nm

	// private Priority priority;// Priority for worker containers - priorities
	// are
	// intra-application
	// private Resource capability; // Resource requirements for worker
	// containers

	// store the request from local am
	// private List<ContainerRequest> arList;

	public remoteAPPMaster(String remoteYarnAddress) throws Exception {

		initProcess(remoteYarnAddress);
	}

	public void initProcess(String remoteYarnAddress) throws Exception {

		conf = new YarnConfiguration();
		// conf.updateConnectAddr(name, addr);
		// init the client from app master to resource manager
		rmClient = AMRMClient.createAMRMClient();
		rmClient.init(conf);
		rmClient.start();

		// Register with ResourceManager
		System.out.println("registerApplicationMaster...");
		rmClient.registerApplicationMaster("", 0, "");
		System.out.println("registerApplicationMaster ok");

		// init the client from app master to node manager
		nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();

		// Priority for worker containers - priorities are intra-application
		// priority = Records.newRecord(Priority.class);
		// priority.setPriority(0);
		// Resource requirements for worker containers
		// capability = Records.newRecord(Resource.class);
		// capability.setMemory(512);
		// capability.setVirtualCores(1);

		/*
		 * 
		 * 
		 * // Make container requests to ResourceManager for (int i = 0; i < n;
		 * ++i) { ContainerRequest containerAsk = new
		 * ContainerRequest(capability, null, null, priority);
		 * System.out.println("Making res-req " + i);
		 * rmClient.addContainerRequest(containerAsk); }
		 * 
		 * 
		 * // Obtain allocated containers, launch and check for responses int
		 * responseId = 0; int completedContainers = 0; while
		 * (completedContainers < n) { AllocateResponse response =
		 * rmClient.allocate(responseId++); for (Container container :
		 * response.getAllocatedContainers()) { // Launch container by create
		 * ContainerLaunchContext ContainerLaunchContext ctx =
		 * Records.newRecord(ContainerLaunchContext.class); ctx.setCommands(
		 * Collections.singletonList( command + " 1>" +
		 * ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>" +
		 * ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" ));
		 * System.out.println("Launching container " + container.getId());
		 * nmClient.startContainer(container, ctx); } for (ContainerStatus
		 * status : response.getCompletedContainersStatuses()) {
		 * ++completedContainers; System.out.println("Completed container " +
		 * status.getContainerId()); } Thread.sleep(100); }
		 * 
		 * // Un-register with ResourceManager
		 * rmClient.unregisterApplicationMaster(
		 * FinalApplicationStatus.SUCCEEDED, "", "");
		 */
	}

	private ContainerRequest translateRequest(ResourceRequest request) {
		ContainerRequest containerAsk = new ContainerRequest(
				request.getCapability(), null, null, request.getPriority());
		return containerAsk;
	}

	public AllocateResponse GetAlloRequest(AllocateRequest request)
			throws YarnException, IOException {
		List<ResourceRequest> allAsk = request.getAskList();
		for(Iterator it2 = allAsk.iterator();it2.hasNext();){
			ResourceRequest tmp = (ResourceRequest) it2.next();
			ContainerRequest containerAsk = translateRequest(tmp);
			System.out.println("");
		    rmClient.addContainerRequest(containerAsk);
		}
	
		AllocateResponse response = rmClient.allocate(request);
		return response;
	}

	public void unregisterAM() throws YarnException, IOException {
		// Un-register with ResourceManager
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
				"", "");
	}

}
